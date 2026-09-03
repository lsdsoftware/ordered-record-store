# Ordered record store working design

Status: **Canonical working design — deliberately not frozen**

Last updated: 2026-09-03

This document is the continuation point for the durable `ordered-record-store` implementation. It supersedes the earlier MySQL-row design and incorporates the useful reasoning formerly kept in `docs/STACK.md`.

The current memory implementation and its tests still expose the earlier interface. They are expected to change. Do not preserve an old API, Messenger assumption, or test merely because it has already been implemented; revise this document deliberately when a better decision is made.

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
- S3 active checkpoints and immutable sealed archives;
- rebuilding its in-memory active catalog at startup;
- lazy discovery and reading of sealed history.

The store does not own:

- users, buddies, conversations, senders, or authorization;
- payload schemas or JSON parsing;
- presence, typing, notifications, or delivery/read receipts;
- permanent request deduplication;
- domain deletion or retention policy;
- a Messenger database table or host-application integration.

The consuming service owns stream-ID derivation, payload encoding, access control, user-facing failure behavior, and any cheap domain-aware duplicate suppression.

## 3. Target public contract

The next breaking revision should retain the conceptual operations:

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

`openSegmentedOrderedRecordStore()` performs all startup reconstruction and returns only when the store is ready. `close()` rejects new work, waits for in-flight per-stream mutations and already-running S3 operations, flushes and closes local handles, and stops timers. It does not force every dirty prefix to S3 during shutdown; local files are already durable and the next startup will queue their checkpoints immediately. The memory adapter implements `close()` as a no-op.

Remove from the target contract:

- `AppendRequest.idempotencyKey`;
- `AppendResult.duplicate`;
- `IDEMPOTENCY_CONFLICT`;
- `deleteStream()`.

No MVP caller needs physical stream deletion. Removing it avoids specifying deletion across local active files, checkpoints, sealed objects, races, and partial failures before a real product requirement exists.

Generate a UUIDv7 for every record using the maintained `uuid` package's `v7()` without an options argument so its timestamp-ID internal state is active. The segment ID is the UUIDv7 of its first record. A public record ID is the unpadded 43-character base64url encoding of 32 bytes:

```text
16-byte record UUIDv7 || 16-byte segment UUIDv7
```

Including the segment ID lets `beforeId` and `afterId` locate the exact active or sealed object without a manifest. `compareRecordIds()` validates and decodes both IDs, then compares the record-UUID half bytewise; callers never compare the base64url text directly. Generated record UUIDs must be strictly increasing within this one writer process, including multiple records in one millisecond; contract tests enforce that property.

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

Ordinary process or host-process crashes recover acknowledged records from the durable local volume. The active S3 checkpoint may lag local storage by up to approximately five minutes. That lag is an explicitly accepted loss window only if the local durable volume itself is catastrophically lost.

Runtime S3 checkpoint failures do not make already fsynced local appends fail. They leave the segment dirty and trigger bounded retry with backoff. Operational visibility should report a growing checkpoint backlog without introducing an enterprise monitoring system.

## 5. Segment and record format

Each active file belongs to one opaque stream and one unique active-segment generation. The segment ID is the UUIDv7 record ID of its first record; a segment is created only as part of its first append, so there are no empty segment files. The stream ID is present in the segment header so path hashes are not the only copy of identity.

Use a custom versioned envelope rather than Protobuf, CBOR, or MessagePack. Those encodings do not provide reverse framing, torn-tail detection, or S3 range-read boundaries, so they would still need the same outer format. The fixed envelope is small enough to implement and test directly, while caller data remains an opaque UTF-8 string.

Version 1 is uncompressed. Future sealed segments may use other codecs without rewriting existing objects because the header identifies the format and codec.

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

The API renders the timestamp as UTC ISO-8601 with millisecond precision. The UUIDv7 timestamp and stored timestamp must agree. CRC32C is implemented in small package-owned TypeScript with published test vectors rather than adding a serialization framework. A sealed/checkpoint object additionally uses a whole-object SHA-256 supplied to S3 with `PutObject`; do not infer integrity from `ETag`.

The store must not add sender, message type, buddy, or conversation fields. Leading length supports forward scan; trailing length supports reverse scan; the checksum detects torn or corrupt records. A segment footer is unnecessary in version 1 because the final valid record frame supplies the tail boundary and sealed-object metadata supplies byte length.

An incomplete final record after a crash is truncated to the last valid frame. Middle-of-file corruption is an error/recovery condition, not silently skipped data.

## 6. Rotation and dormancy

An active segment is sealed under either condition:

- after a complete append makes it approximately 256 KiB or larger; or
- it has received no successful append for 14 days.

The size is a target, not a hard boundary. Append the complete record and then test rotation. Overshoot by one record—even 50 KiB or more—is acceptable and simpler than splitting or preflighting payloads.

The idle condition measures time since the last successful append. Unlike checkpoint timing, every append moves the idle deadline. A simple hourly sweep of the in-memory active catalog is adequate; one operating-system timer per stream is unnecessary. Startup reads the last valid record timestamp and immediately seals any active segment already idle for 14 days.

A sealed segment is immutable forever. After a successful seal, the stream becomes dormant and has no empty successor active segment. If a record arrives weeks or years later, create a brand-new independently identified active segment. Do not reopen, append to, inspect, or download the newest sealed segment merely to restart writing.

This rule is essential to bounding startup work. Creating an empty active anchor after every idle seal would retain one active file/object per historical stream and defeat idle sealing.

Segment and record identities therefore never require reading the previous segment to calculate a next sequence number. The first new UUIDv7 record becomes the new segment ID.

## 7. Active checkpoint scheduling

An active segment has a checkpointed prefix length and a possibly longer local length.

Checkpoint scheduling follows audit-time behavior:

1. The first append that makes a clean segment dirty sets a deadline five minutes later.
2. Later appends before that deadline do not move it.
3. At the deadline, capture the current byte length, last included record ID, and checksum needed by the upload operation.
4. Upload exactly that captured prefix while later local appends may continue.
5. The first append after capture begins the next five-minute window, even if the prior upload is still completing.
6. A failed upload keeps the captured prefix dirty and retries it with backoff; failure does not restart a fresh five-minute grace period.

Only one checkpoint upload for a segment needs to run at a time. A later due checkpoint may coalesce to the newest safe prefix once the prior operation settles.

If the segment reaches a seal condition before its checkpoint deadline, cancel the ordinary checkpoint and seal/upload the full segment immediately.

The S3 checkpoint object for one active-segment generation is overwritten at a stable generation-specific key. Object replacement must never expose a partial object. Each new active generation uses a new key so a stale checkpoint with coincidentally equal length cannot be mistaken for the new file.

## 8. S3 layout and sealing

For an opaque UTF-8 stream ID, calculate lowercase hexadecimal `SHA-256(streamId)`. Use its first two hex characters as the shard and the remaining 62 as the stream directory. UUID values in paths are lowercase 32-character hex without hyphens. The key layout is:

```text
active/v1/<shard>/<remaining-stream-hash>/<segment-id>.ors
segments/v1/<shard>/<remaining-stream-hash>/<inverted-segment-id>-<segment-id>.ors
```

`inverted-segment-id` is the lowercase hex encoding of the bitwise complement of the 16 segment-ID bytes. A general-purpose S3 bucket lists keys lexicographically, so the newest segment sorts first and `MaxKeys=1` finds it. Directory/S3 Express buckets are not supported because they do not provide this ordering. A cursor contains the original segment ID, so its exact sealed key is derivable without listing.

The path hash avoids exposing caller identifiers and produces safe bounded keys. The segment header retains the original stream ID and detects collisions or misplaced files.

Key-format invariants:

- every active generation has a unique identity;
- checkpoints for that generation overwrite only its own active key;
- sealed keys are immutable and deterministic enough that retrying an uncertain seal is safe;
- sealed objects for one stream list newest-first, allowing the latest sealed segment to be found without listing the stream's entire history;
- a record cursor identifies its segment, allowing older-page lookup without a separate manifest.

Seal under the stream's serialized operation queue:

1. close/capture the complete valid local segment;
2. upload it to its deterministic immutable sealed key;
3. verify the completed upload using a whole-segment checksum;
4. delete the matching S3 active checkpoint;
5. delete the local active file; version 1 has no sealed local cache;
6. remove the stream from the active runtime catalog.

If upload completion is uncertain, repeating the immutable upload with identical bytes is safe. Delete neither the active checkpoint nor the local active file until the sealed object is verified.

Deleting the active checkpoint before the local file ensures an ordinary crash cannot leave an S3-only active segment during sealing. A crash after checkpoint deletion but before local deletion can cause the local file to be rediscovered; because it is already at the size/idle seal condition and the sealed key is deterministic, startup safely repeats the seal and cleanup.

Active checkpoints remain in S3 Standard. The recommended versioning and sealed-object lifecycle policy is recorded in section 15; it is deployment policy rather than part of the record API.

## 9. No manifest and no persisted catalog

There is intentionally:

- no per-stream manifest;
- no persisted runtime catalog;
- no dirty-marker sidecar.

At runtime an in-memory catalog tracks active file handles, lengths, last records, last-append times, checkpoint state, and per-stream serialization. The catalog is disposable and rebuilt on every process start.

Only the active frontier is rebuilt eagerly. Sealed archives are never enumerated globally at startup. They are discovered lazily for one stream when a read crosses into sealed history or asks for the latest record of a dormant stream.

Idle sealing makes startup work proportional to streams used within roughly the last 14 days, rather than every stream ever created. A stream kept active by a recent append may still contain older records, but every active segment remains bounded to approximately the rotation target plus one record.

Persisting a catalog would not remove the need to reconcile it with local files after an unclean shutdown. It would create another crash-consistency protocol while saving limited work once idle sealing bounds the frontier.

## 10. Startup reconstruction and reconciliation

The durable store must finish active-frontier reconstruction before Messenger advertises readiness. Failure to complete the full S3 `active/` listing, or any required exceptional checkpoint restore, makes `openSegmentedOrderedRecordStore()` reject. The Messenger composition root logs a content-free operational error and exits nonzero; its process supervisor may retry later. There is no startup degraded mode.

Startup performs:

1. Enumerate every local active file.
2. Validate its header and framed records; truncate an incomplete final frame.
3. Rebuild each local active catalog entry, including valid byte length and last-append time.
4. Perform paginated S3 listing of the complete `active/` prefix.
5. Build a remote map from unique active-segment key to listed object size.
6. Reconcile the union of local and remote generations.
7. Queue dirty generations immediately rather than granting a new five-minute grace period.
8. Immediately seal generations already over the size target or idle for 14 days.

Normal reconciliation needs S3 listing metadata only. It does not download every active checkpoint and does not rely on `ETag` as a universal content checksum.

| Local state | Listed S3 active state | Startup action |
| --- | --- | --- |
| Same generation and same byte length | Present | Treat as clean after local frame validation |
| Local file is longer | Shorter object | Treat as dirty; checkpoint immediately |
| Local file exists | No object | Treat as dirty; checkpoint immediately |
| No local file | Object exists | Exceptional recovery: download and restore it |
| Local file is shorter | Longer object | Exceptional recovery: download/restore the remote prefix |

The equal-length case relies on the single-writer invariant and the rule that an active checkpoint is always an exact prefix of that same uniquely identified local generation. Local frame checks detect ordinary torn/corrupt tails. A separate full remote-byte verification on every restart is unnecessary; the upload was checksummed when written.

Remote-only or remote-longer cases should not occur on an ordinary restart with the same durable local volume. They cover catastrophic local loss, replaced storage, or corruption and are the only startup cases that require checkpoint downloads.

If the service was offline for more than 14 days, its first restart may discover and seal many old active segments once. Subsequent startups benefit from the bounded frontier.

## 11. Reads and lazy archive discovery

Active files support forward scan and reverse scan through their framing. Sealed version-1 objects remain uncompressed so the durable adapter can use range reads near the object tail rather than download an entire segment merely to return a recent page.

For a dormant stream, `append()` creates a new active generation without consulting S3. A read or `getLatest()` independently lists that stream's sealed prefix with `MaxKeys=1`, range-reads the returned object's tail, and caches the result in memory.

A `beforeId` or `afterId` cursor contains its segment UUID, so the store derives the exact active and sealed keys directly. When backward pagination exhausts that segment, one newest-first prefix listing starting after its inverted segment key finds the immediately older sealed segment. It does not enumerate the stream's entire history.

Optional per-segment offset indexes or local sealed caches may be introduced only after measuring read performance. They are not required for the initial approximately 256 KiB segments.

`getLatest(streamIds)` for many dormant streams may translate into several lazy S3 lookups. For the MVP, perform them with bounded concurrency and do not add a conversation-head table or generic global manifest. Measure real snapshot latency before adding another projection.

## 12. Duplicate handling

The store does not accept or persist idempotency keys. A durable idempotency index would require an index, sidecar, or scan and its own recovery protocol solely to hide rare retry duplicates.

Messenger retains `clientMessageId` inside its Messenger-owned opaque payload and serializes appends per conversation. Before append, it may compare the current head: the same sender and client message ID returns the head record. If an intervening message means the original is no longer the head, or an uncertain append cannot be found cheaply, a duplicate may be appended. This is acceptable.

Comparing text alone is insufficient because legitimate consecutive messages such as `ok`, `?`, or the same emoji must remain possible. A client message ID is cheap and avoids suppressing intentional repeats without becoming a permanent store index.

The browser should not automatically retry uncertain sends. Realtime client deduplication by canonical server record ID remains useful and is unrelated to append idempotency.

## 13. Failure and concurrency boundaries

- One process owns the local store and S3 active namespace at a time.
- Appends and seal/checkpoint state transitions are serialized per stream.
- Different streams may append and checkpoint concurrently with bounded concurrency.
- A process crash may leave a partial local tail; startup truncates it.
- A process crash during an active checkpoint leaves either the old or new complete S3 object; local length determines whether another checkpoint is needed.
- A process crash during sealing safely retries the deterministic immutable upload and cleanup.
- Local durable-volume loss restores active prefixes from S3 and may lose the accepted five-minute tail.
- Sealed archive loss is outside the module's ordinary recovery model and is handled through S3 durability/versioning/backup policy.
- S3 unavailability during normal runtime leaves local segments dirty and retrying; already-fsynced local appends may continue while disk capacity remains.
- If the process restarts while S3 remains unavailable, startup fails and the process exits. This simple fail-closed rule prevents serving from an unreconciled active frontier.

## 14. Initial implementation sequence

1. Revise the public interface, memory adapter, shared contract tests, and package documentation to remove permanent idempotency and deletion.
2. Revise Messenger payload/deduplication assumptions and move private read positions to compact Messenger-owned MySQL state.
3. Implement the documented record IDs/cursors, binary layout, segment IDs, key layout, and async open/close shape as contract tests.
4. Implement and exhaustively test binary framing, reverse scans, checksum validation, and torn-tail recovery without S3.
5. Implement the local durable adapter with per-stream serialization, fsynced append, size rotation, and idle sweep.
6. Inject an object-store boundary and test checkpoint audit timing, captured-prefix uploads, retries, and deterministic sealing against a fake implementation.
7. Add the S3 implementation, full active-prefix listing reconciliation, exceptional downloads, and lazy sealed range reads.
8. Run the same behavior suite against memory and durable adapters plus restart/fault tests.
9. Integrate the durable adapter into Messenger, gate broker readiness on reconstruction, and rehearse restart/offline-history behavior.
10. Document local volume, S3 bucket/lifecycle/versioning, recovery, and deployment configuration before production cutover.

## 15. Recommended implementation defaults

The startup-failure and binary-format choices above were explicitly accepted on 2026-09-03. Treat the remaining recommendations below as implementation defaults unless later evidence causes a deliberate revision:

- **Object-store boundary:** define a small internal interface for list, put, range/full get, head, and delete. Test it with an in-memory fake. The production implementation wraps an injected AWS SDK v3 `S3Client`; credentials and region remain composition-root concerns.
- **S3 concurrency:** use one configurable limiter with a default of four in-flight object operations. Prioritize seal/checkpoint writes over user-triggered archive reads. The active-prefix startup listing remains sequentially paginated.
- **Bucket kind:** require a general-purpose S3 bucket. The newest-first key design relies on lexicographical `ListObjectsV2` ordering and therefore does not support S3 Express directory buckets.
- **Local volume:** require an explicit absolute data-directory path on persistent local SSD/block storage. Do not silently use the process working directory, `/tmp`, a container layer, or NFS. Store version-1 active files under `<dataDirectory>/v1/active/<shard>/<stream-hash>/<segment-id>.ors`.
- **Disk capacity:** begin with at least 10 GiB for the active volume and an ordinary host disk-space alarm. The store reports checkpoint backlog and free-space diagnostics, but it does not build an automatic eviction system. `ENOSPC` fails append rather than acknowledging an unflushed record.
- **Sealed local cache:** none for the MVP. Delete the local active file after the immutable sealed upload is verified and the active checkpoint is removed. Use S3 range reads; add a bounded cache only after measured latency justifies it.
- **Private read state:** move Messenger read positions to one mutable MySQL row per directional `(accountId, buddyId)` pair. This is compact current state, not append-only history, and does not belong in tiny ordered-record streams.
- **Conversation-head projection:** do not add one initially. Use bounded lazy `getLatest()` S3 lookups for dormant conversations and measure snapshot latency before accepting dual-write/projection complexity.
- **S3 versioning/lifecycle:** use bucket versioning. Keep current `active/` objects in Standard and expire their noncurrent versions after one day. Keep current `segments/` objects in Standard for 30 days, then transition them to Standard-IA; expire noncurrent sealed-object versions after 30 days. Never expire current sealed objects. Clean expired delete markers.
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
2. Read `diepkhuc-messenger/docs/MVP-CONTRACT.md`, especially its storage mapping and current implementation status.
3. Inspect `src/index.ts` and its tests, remembering that they implement the older draft API.
4. Inspect how `apsvc-diepkhuc-messenger` currently uses idempotency, decimal IDs, `getLatest()`, and read-position streams.
5. Start with the section 15 defaults and record any deliberate revision here before relying on it in code.
6. Keep the durable adapter domain-neutral and resist copying the abandoned Messenger v2 S3 implementation wholesale.
