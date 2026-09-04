# Ordered record store working design

Status: **Canonical working design — deliberately not frozen**

Last updated: 2026-09-04

This document is the continuation point for the durable `ordered-record-store` implementation. It supersedes the earlier MySQL-row design and incorporates the useful reasoning formerly kept in `docs/STACK.md`.

The memory and segmented local/S3 adapters now implement this interface and
format. The design remains deliberately thawed: do not preserve an API,
Messenger assumption, or implementation merely because it has shipped in this
working branch; revise this document deliberately when a better decision is
made.

## 1. Purpose and scale

DiepKhuc needs durable one-to-one message history, but the storage boundary must not depend on Messenger, buddies, accounts, or the host application's structure. The module stores ordered opaque strings in opaque streams.

The site has a few thousand users and one operator. The design should be robust and recoverable without becoming a distributed storage platform.

The motivating scale is historical: existing coin activity tables have reached roughly 100 million rows over eight years. A sustained message per second is about 31.5 million records per year. Chat history is append-heavy, sequential, rarely queried deeply, and needs no relational joins. Turning every message into a database row would buy query and transaction machinery that this workload does not need.

The target is therefore an in-process TypeScript ESM module backed by append-only local segment files and S3—not another network service and not a row-per-record MySQL table.

## 2. Responsibility boundary

The store owns:

- appending opaque string data to opaque stream IDs;
- assigning canonical record IDs and timestamps;
- ordered newest, older, and forward reads;
- local segment framing, flushing, recovery, and rotation;
- checkpointing local working segments into a single S3 segment namespace;
- rebuilding its in-memory local working-set catalog at startup;
- lazy discovery, download, and reading of nonlocal segments.

The store does not own:

- users, buddies, conversations, senders, or authorization;
- payload schemas or JSON parsing;
- presence, typing, notifications, or delivery/read receipts;
- permanent request deduplication;
- domain deletion or retention policy;
- a Messenger database table or host-application integration.

The consuming service owns stream-ID derivation, payload encoding, access control, user-facing failure behavior, and any cheap domain-aware duplicate suppression.

## 3. Public contract

The implemented contract exposes:

```ts
interface StoredRecord {
  readonly id: string
  readonly streamId: string
  readonly data: string
  readonly createdAt: string
}

interface OrderedRecordStore {
  append(request: { streamId: string; data: string }): Promise<StoredRecord>
  read(request: {
    streamId: string
    beforeId?: string
    afterId?: string
    limit: number
  }): Promise<StoredRecord[]>
  getLatest(streamIds: readonly string[]): Promise<Map<string, StoredRecord | null>>
  close(): Promise<void>
}

declare function compareRecordIds(left: string, right: string): -1 | 0 | 1

declare function openSegmentedOrderedRecordStore(
  options: SegmentedOrderedRecordStoreOptions,
): Promise<OrderedRecordStore>
```

`openSegmentedOrderedRecordStore()` reconstructs and validates local files, queues locally dirty files for checkpointing, and returns when that local reconstruction is complete. It performs no S3 availability check, listing, or download during startup. `close()` rejects new work, waits for in-flight per-stream mutations and already-running S3 operations, flushes and closes local handles, and stops timers. It does not force every dirty file to S3 during shutdown; local files are already durable and the next startup will queue their checkpoints immediately. The memory adapter implements `close()` as a no-op.

Remove from the target contract:

- `AppendRequest.idempotencyKey`;
- `AppendResult.duplicate`;
- `IDEMPOTENCY_CONFLICT`;
- `deleteStream()`.

No MVP caller needs physical stream deletion. Removing it avoids specifying deletion across local files, S3 segment objects, races, and partial failures before a real product requirement exists.

Generate a UUIDv7 for every record using the maintained `uuid` package's `v7()` without an options argument so its timestamp-ID internal state is active. The segment ID is the UUIDv7 of its first record. A public record ID is the unpadded 43-character base64url encoding of 32 bytes:

```text
16-byte record UUIDv7 || 16-byte segment UUIDv7
```

Including the segment ID lets `beforeId` and `afterId` locate the exact local or S3 segment without a manifest. `compareRecordIds()` validates and decodes both IDs, then compares the record-UUID half bytewise; callers never compare the base64url text directly. Generated record UUIDs must be strictly increasing within this one writer process, including multiple records in one millisecond; contract tests enforce that property.

Per-record UUIDv7 is necessary because a segment can remain active for days. A new record in an old segment must still sort newer than records written yesterday to other streams. The host clock must remain NTP-synchronized; handling a clock that moves materially backward across a complete process restart is outside the MVP reliability model.

All adapters must enforce common identifier, data, and page-size limits. The current limits—64 UTF-8 bytes for stream IDs, 65,535 UTF-8 bytes for data, and 1,000 records per read—are reasonable defaults but remain reviewable while the interface is thawed.

`beforeId` and `afterId` are mutually exclusive and must be actual record IDs previously returned for the same stream. This tightens the old memory behavior, where an arbitrary numeric boundary happened to work, and enables direct segment lookup from the durable cursor.

## 4. Durability model

There is one writer process for the MVP. Multiple storage writers, distributed locks, shared network filesystems, cross-region failover, and five-nines availability are out of scope.

Successful append means:

1. construct one complete framed record;
2. append it to the stream's local active segment;
3. call the Node `FileHandle.sync()` equivalent of `fsync` on the local file;
4. return the canonical record.

Only after step 3 may Messenger acknowledge or fan out the canonical send. `datasync()` is not used for this contract because Node documents that it does not flush modified metadata; the expected message rate does not justify weakening the simpler `sync()` rule. S3 is asynchronous and never delays the request path.

Ordinary process or host-process crashes recover acknowledged records from the durable local volume. The newest S3 segment snapshot may lag local storage by up to approximately five minutes. That lag is an explicitly accepted loss window only if the local durable volume itself is catastrophically lost.

Runtime S3 checkpoint failures do not make already fsynced local appends fail. They leave the segment dirty and trigger bounded retry with backoff. Operational visibility should report a growing checkpoint backlog without introducing an enterprise monitoring system.

## 5. Segment and record format

Each local file belongs to one opaque stream and one unique segment. The segment ID is the UUIDv7 record ID of its first record; a segment is created only as part of its first append, so there are no empty segment files. The stream ID is present in the segment header so path hashes are not the only copy of identity.

Use a custom versioned envelope rather than Protobuf, CBOR, or MessagePack. Those encodings do not provide record framing, torn-tail detection, or per-record integrity, so they would still need the same outer format. The fixed envelope is small enough to implement and test directly, while caller data remains an opaque UTF-8 string.

Version 1 is uncompressed. Future segments may use other codecs without rewriting existing objects because the header identifies the format and codec.

All integers are unsigned big-endian. The version-1 segment header is:

```text
8 bytes   magic ASCII "ORSSEG01"
4 bytes   total header length, including magic and header CRC32C
2 bytes   format version = 1
1 byte    codec = 0 (none)
1 byte    flags = 0
16 bytes  segment UUIDv7 (same as first record ID)
8 bytes   segment-created Unix epoch milliseconds
2 bytes   stream-ID UTF-8 byte length
N bytes   opaque stream-ID UTF-8 bytes
4 bytes   CRC32C of bytes from total header length through stream-ID bytes
```

The version-1 record frame is:

```text
4 bytes   body length = 32 + payload byte length
16 bytes  record UUIDv7
8 bytes   canonical Unix epoch milliseconds
4 bytes   payload UTF-8 byte length
N bytes   opaque payload UTF-8 bytes
4 bytes   CRC32C of record UUID through payload bytes
4 bytes   repeated body length
```

The API renders the timestamp as UTC ISO-8601 with millisecond precision. The UUIDv7 timestamp and stored timestamp must agree. CRC32C is implemented in small package-owned TypeScript with published test vectors rather than adding a serialization framework. Every uploaded segment snapshot additionally uses a whole-object SHA-256 supplied to S3 with `PutObject`; do not infer integrity from `ETag`.

The store must not add sender, message type, buddy, or conversation fields. Leading length supports forward scan; trailing length supports reverse scan; the checksum detects torn or corrupt records. A segment footer is unnecessary in version 1 because the final valid record frame supplies the tail boundary and S3 object metadata supplies byte length.

An incomplete final record after a crash is truncated to the last valid frame. Middle-of-file corruption is an error/recovery condition, not silently skipped data.

## 6. Rotation and local residency

The newest segment of a stream is mutable whether or not it currently has a local copy. When rotation creates a newer segment, every earlier segment becomes immutable. There is no separate sealing operation or remote active-segment class.

Before appending a record, inspect the current local segment. If its valid length is already approximately 256 KiB or larger, the new record begins a new segment. Because the threshold is tested only between complete records, the prior segment may exceed the target by one record—even by 50 KiB or more. That is acceptable and simpler than splitting or preflighting payloads.

A segment is created only with its first record. The first record's UUIDv7 is also the new segment ID, so rotation never creates an empty successor and never needs a sequence value from the previous segment.

Fourteen days of append inactivity controls only local cache residency. A simple hourly sweep is sufficient; one timer per stream is unnecessary. A clean current local file with no successful append for 14 days may be deleted and removed from the in-memory catalog. In normal continuous operation it became clean roughly five minutes after its last append. A dirty-and-idle file is an exceptional recovery case after an unusually long process interruption or S3 outage; upload it successfully and mark it clean before deletion. If S3 is unavailable, retain it and retry rather than deleting acknowledged data.

When a later append finds no local file, it lazily locates the newest S3 object for that stream. If the listed object is below the rotation target, download and validate it as the clean local working file, then append. If it is already at or above the target, create a new segment without downloading the old object.

This local eviction rule bounds startup work to cached recent streams and any dirty backlog. It does not create extra small S3 objects merely because a conversation was idle.

## 7. Local dirty state and checkpoint scheduling

A local segment is either clean, meaning its complete valid bytes match the most recently successful S3 upload for that key, or dirty, meaning S3 may contain only an earlier prefix or no object yet. This bit must survive process failure; it is per-file recovery metadata, not a persisted stream catalog.

The initial implementation encodes the state in an atomically renamed filename suffix, for example `<segment-id>.clean.ors` and `<segment-id>.dirty.ors`. Before modifying a clean file, rename it to dirty and fsync the parent directory. Only then append and `FileHandle.sync()` the complete record. A new segment is installed as dirty with both its file and directory entry durably flushed as part of its first append. A segment downloaded from S3 is installed as clean through a validated, fsynced temporary file and atomic rename before it can be modified.

Checkpoint scheduling follows audit-time behavior:

1. The first append that makes a clean segment dirty sets a deadline five minutes later.
2. Later appends before that deadline do not move it.
3. At the deadline, capture the current valid byte length and whole-prefix checksum.
4. Upload exactly that captured prefix to the segment's stable S3 key while later local appends may continue.
5. If no later bytes were appended, atomically rename the local file clean and fsync its parent directory. If the file grew, leave it dirty and let the first append after capture establish the next five-minute window.
6. A failed upload leaves the file dirty and retries with backoff; failure does not grant a fresh five-minute grace period.

Only one upload for a particular segment may run at a time. A later due checkpoint may coalesce to the newest safe prefix after the earlier operation settles. This prevents an older upload from completing after a newer upload and regressing the S3 object.

When rotation makes a segment noncurrent, queue its final full upload immediately. The new segment may accept local appends while that upload runs. Delete the old local file only after its final upload succeeds and its state is clean; retain it through any S3 outage.

S3 publication remains ordered per stream: the final upload of an older segment must succeed before the first upload of the newer segment. Local appends to the newer file do not wait. This prevents newest-first discovery from exposing a newer S3 segment while its immediate predecessor is still absent. After a restart, upload multiple dirty files for one stream from oldest to newest.

S3 `PutObject` replaces the entire object at the stable segment key. It is not an in-place append. Object replacement must expose either the old complete prefix or the new complete prefix, never a partial object.

## 8. S3 layout and segment publication

For an opaque UTF-8 stream ID, calculate lowercase hexadecimal `SHA-256(streamId)`. Use its first two hex characters as the shard and the remaining 62 as the stream directory. UUID values in paths are lowercase 32-character hex without hyphens. There is one S3 namespace:

```text
segments/v1/<shard>/<remaining-stream-hash>/<inverted-segment-id>-<segment-id>.ors
```

There is deliberately no `active/v1/` prefix. The current segment's object is overwritten by periodic snapshots. After a newer segment is created, the older object's final successful upload is immutable by rule.

`inverted-segment-id` is the lowercase hex encoding of the bitwise complement of the 16 segment-ID bytes. A general-purpose S3 bucket lists keys lexicographically, so the newest segment sorts first and a stream-specific `ListObjectsV2` with `MaxKeys=1` finds it. Directory/S3 Express buckets are not supported because they do not provide this ordering. A cursor contains the original segment ID, so its exact object key is derivable without listing.

The path hash avoids exposing caller identifiers and produces safe bounded keys. The segment header retains the original stream ID and detects collisions or misplaced files.

Key-format invariants:

- every segment has a unique stable identity and one S3 key;
- periodic snapshots overwrite only that segment's key;
- an older segment is never modified after a newer segment exists;
- segments for one stream list newest-first without enumerating the stream's entire history;
- a record cursor identifies its segment, allowing exact lookup without a manifest.

The bucket dedicated to this store has versioning disabled so periodic overwrites do not retain an unbounded chain of noncurrent checkpoint objects. Segment objects may use Standard or an immediately readable class such as Standard-IA. They must never transition to Glacier or another storage class that requires a restore before `GetObject`. Current objects are not expired by lifecycle policy.

## 9. No manifest or persisted catalog

There is intentionally:

- no per-stream manifest;
- no persisted runtime catalog;
- no remote active-segment prefix or global active-frontier index.

There is a durable clean/dirty state beside each local segment, encoded in its filename. That state answers only whether the local bytes need uploading; it does not enumerate remote streams or record stream history.

At runtime an in-memory catalog tracks locally resident files, their lengths and last records, last-append times, checkpoint state, and per-stream serialization. The catalog is disposable and rebuilt from local files on every process start. It is intentionally not a complete catalog of streams in S3.

Nonlocal segments are never enumerated globally at startup. They are discovered lazily for one stream when a read crosses into nonlocal history, `getLatest()` needs a nonlocal head, or an append has no local working file.

Persisting a full catalog would create another crash-consistency protocol without eliminating local-file inspection. The filename-level dirty bit is the only persisted runtime state required by this design.

## 10. Startup reconstruction

Startup is local-only and performs:

1. Enumerate every local clean and dirty segment file.
2. Validate its header and framed records. Truncate an incomplete final frame only in a dirty file. An invalid clean cache file is never uploaded; remove or quarantine it so its S3 object can be rediscovered lazily. Middle-of-file corruption remains an explicit recovery error.
3. Rebuild catalog entries for the local working set and identify the newest local segment per stream.
4. Queue every dirty file for immediate upload rather than granting a new five-minute grace period.
5. Delete any clean noncurrent local segment, and queue final upload then cleanup for a dirty noncurrent segment left by a crash or prior S3 outage.
6. Evict clean current files already idle for 14 days.

`openSegmentedOrderedRecordStore()` returns after this local reconstruction. Readiness performs no S3 availability probe, list, head, or download and succeeds while S3 is unavailable. Queued dirty uploads are ordinary asynchronous runtime work and may begin immediately, but opening does not wait for them. Operations that can be satisfied from local files continue; an operation that needs a nonlocal segment fails normally if S3 cannot provide it.

The store trusts a clean local marker under the single-writer invariant. It does not compare every clean file with S3 during startup. Restoring the local data directory to an older snapshot while retaining newer S3 state is outside the MVP recovery model because a stale local file could overwrite a newer remote prefix. After total local-volume loss or deliberate rollback, discard the local working set and let streams rehydrate lazily from S3.

## 11. Reads and lazy segment discovery

Local files support forward and reverse scan through their framing. Nonlocal version-1 objects remain uncompressed and are always fetched as complete objects. At the approximately 256 KiB target, an extra range-read path saves little bandwidth while making later pagination likely to issue another request.

For an append with no local working file, serialize the stream operation and issue a newest-first `ListObjectsV2` for that stream with `MaxKeys=1`:

- if no object exists, create a new dirty local segment with the appended record;
- if the newest object's listed size is at or above the rotation target, create a new dirty local segment without downloading it;
- otherwise download the whole newest segment, validate and install it as clean locally, then mark it dirty and append.

Downloading the whole object on the last path is intentional: the segment is only approximately 256 KiB, and its next checkpoint must upload a complete replacement object. Concurrent cache-miss work and all later mutations for that stream use the same per-stream serialization.

A read or `getLatest()` uses a local segment when present. Otherwise it fetches, validates, and decodes the complete object, then retains it in a process-local 64-segment least-recently-used cache. Continued pagination and a later append can reuse those bytes without another `GetObject`; an append installs a reusable below-target object as the clean local working file before modifying it. The cache is disposable, is not scanned at startup, and is invalidated whenever this process successfully overwrites the corresponding S3 object.

A `beforeId` or `afterId` cursor contains its segment UUID, so the store derives the exact local or S3 key directly. When backward pagination exhausts that segment, one newest-first prefix listing starting after its inverted segment key finds the immediately older segment. It does not enumerate the stream's entire history.

Optional per-segment offset indexes may be introduced only after measuring performance. They are not required for the initial approximately 256 KiB segments or its small bounded decoded-object cache.

`getLatest(streamIds)` for many nonlocal streams may translate into several lazy S3 lookups. For the MVP, perform them with bounded concurrency and do not add a conversation-head table or generic global manifest. Measure real snapshot latency before adding another projection.

If S3 is unavailable, cached local appends and reads continue. A cache-miss append or read that requires a nonlocal segment fails so the consuming application can present its ordinary temporary-failure behavior.

## 12. Duplicate handling

The store does not accept or persist idempotency keys. A durable idempotency index would require an index, sidecar, or scan and its own recovery protocol solely to hide rare retry duplicates.

Messenger retains `clientMessageId` inside its Messenger-owned opaque payload and serializes appends per conversation. Before append, it may compare the current head: the same sender and client message ID returns the head record. If an intervening message means the original is no longer the head, or an uncertain append cannot be found cheaply, a duplicate may be appended. This is acceptable.

Comparing text alone is insufficient because legitimate consecutive messages such as `ok`, `?`, or the same emoji must remain possible. A client message ID is cheap and avoids suppressing intentional repeats without becoming a permanent store index.

The browser should not automatically retry uncertain sends. Realtime client deduplication by canonical server record ID remains useful and is unrelated to append idempotency.

## 13. Failure and concurrency boundaries

- Exactly one process/service instance owns the local store and S3 prefix. Multiple writers and distributed ownership are not supported.
- Appends, rotation decisions, local state changes, and checkpoint scheduling are serialized per stream. Different streams may operate concurrently with bounded S3 concurrency.
- At most one PUT per segment may be in flight, and different segments for one stream are first published oldest-to-newest, so remote history cannot expose a newer segment while its predecessor is absent.
- A process crash may leave a partial tail in a dirty local file; startup truncates it and keeps the file dirty. An invalid clean cache file is discarded or quarantined rather than overwriting its authoritative S3 object.
- A process crash during `PutObject` leaves either the old or new complete S3 object. The durable dirty marker causes a safe repeat upload when completion was not recorded locally.
- A crash after a successful PUT but before the clean rename also causes only a redundant upload.
- Catastrophic local-volume loss may lose the accepted tail since the last successful checkpoint. Streams and their last uploaded segment are rediscovered lazily from S3.
- Restoring an old local-volume snapshot over newer S3 objects is unsupported. Discard an intentionally rolled-back cache instead of allowing stale local files to overwrite S3.
- S3 unavailability during normal runtime leaves local files dirty and retrying; already-fsynced appends to cached streams may continue while disk capacity remains.
- Startup itself does not contact S3 and therefore does not fail merely because S3 is unavailable. Cache-miss operations that require S3 fail until it recovers.
- Loss of S3 segment objects is outside the module's ordinary recovery model and belongs to bucket durability and external backup policy.

## 14. Implementation status and remaining sequence

As of 2026-09-04, the package implements the public contract, memory adapter,
UUIDv7 cursor, binary codec, durable local adapter, object-store boundary, S3
adapter, lazy reads, checkpointing, rotation, restart recovery, and idle
eviction. The suite runs the shared behavior contract against both adapters and
covers CRC32C, torn dirty tails, local-only startup, audit timing, append/upload
overlap, upload retry, ordered rotation, remote rehydration and cached whole-object
reads, clean cache corruption, and idle eviction.

Messenger's payload revision, best-effort head deduplication, compact MySQL read
positions, composition-root selection, and restart test are implemented in the
consuming backend. Remaining work is deployment-specific:

1. Create the Messenger read-position table in the deployment database.
2. Provision the dedicated bucket and persistent local data directory.
3. Choose a reproducible package reference instead of the development `file:`
   dependency.
4. Rehearse the service against the real development broker, database, local
   volume, and bucket, including restart and temporary S3 failure.
5. Configure disk/backlog visibility and write the final operator recovery notes
   for the chosen host paths.

## 15. Recommended implementation defaults

The binary format and single-prefix mutable-latest design above were explicitly accepted by 2026-09-04. Treat the remaining recommendations below as implementation defaults unless later evidence causes a deliberate revision:

- **Object-store boundary:** define a small internal interface for list, put, and complete-object get. Test it with an in-memory fake. The production implementation wraps an injected AWS SDK v3 `S3Client`; credentials and region remain composition-root concerns.
- **S3 concurrency:** use one configurable limiter with a default of four in-flight object operations. Prioritize dirty/final segment uploads over user-triggered nonlocal reads. Serialize uploads per segment.
- **Bucket kind:** require a general-purpose S3 bucket. The newest-first key design relies on lexicographical `ListObjectsV2` ordering and therefore does not support S3 Express directory buckets.
- **Local volume:** require an explicit absolute data-directory path on persistent local SSD/block storage. Do not silently use the process working directory, `/tmp`, a container layer, or NFS. Store version-1 files under `<dataDirectory>/v1/segments/<shard>/<stream-hash>/<segment-id>.clean.ors` or `<segment-id>.dirty.ors`.
- **Disk capacity:** begin with at least 10 GiB for the local working set and an ordinary host disk-space alarm. Evict clean current files after 14 idle days and delete clean noncurrent files after final upload. Retain dirty files until upload succeeds. `ENOSPC` fails append rather than acknowledging an unflushed record.
- **Segment caches:** retain the current below-target segment locally while active. After 14 days without append, evict it if clean; the dirty-and-idle case is exceptional and must finish uploading before eviction. Fetch complete nonlocal objects and keep at most 64 in a disposable in-memory LRU so continued pagination does not repeat `GetObject` for the same segment.
- **Private read state:** move Messenger read positions to one mutable MySQL row per directional `(accountId, buddyId)` pair. This is compact current state, not append-only history, and does not belong in tiny ordered-record streams.
- **Conversation-head projection:** do not add one initially. Use bounded lazy `getLatest()` S3 lookups for nonlocal conversations and measure snapshot latency before accepting dual-write/projection complexity.
- **S3 versioning/lifecycle:** use a dedicated bucket with versioning disabled for this store. Keep segment objects indefinitely. Standard is sufficient; an optional transition to Standard-IA is allowed because reads remain immediate. Never transition these objects to Glacier or any restore-required archival tier.
- **Object integrity:** use single-request `PutObject` with a supplied full-object SHA-256 for these sub-megabyte objects. Do not use multipart upload and do not treat `ETag` as a checksum.
- **Package deployment:** during the solo-development phase, depend on an exact Git commit SHA so deployments are reproducible without adding a package-release workflow. Publish a normal semver npm package only when another consumer or release process makes that worthwhile.

## 16. Explicit non-goals

- multiple simultaneous writers or service instances;
- distributed consensus, Redis coordination, or cross-region failover;
- transactions spanning streams;
- full-text search or relational querying of payloads;
- per-record redaction or arbitrary historical mutation;
- storage-level permanent idempotency;
- group-chat domain behavior;
- attachments or media storage;
- a generic record-retention/deletion API;
- compressing version-1 segments before a measured need.

## 17. Resume instructions

1. Read this document completely and then `README.md`.
2. Read `../../../DiepKhucProjects/apsvc-diepkhuc-messenger/docs/MVP-CONTRACT.md`, especially its storage mapping and current implementation status.
3. Inspect `src/index.ts` and `test/index.test.mjs`; they implement the current candidate contract and both adapters.
4. Inspect how `apsvc-diepkhuc-messenger` uses head-only retry suppression, opaque message cursors, `getLatest()`, and MySQL read positions.
5. Start with the section 15 defaults and record any deliberate revision here before relying on it in code.
6. Keep the durable adapter domain-neutral and resist copying the abandoned Messenger v2 S3 implementation wholesale.
