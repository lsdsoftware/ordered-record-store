# ordered-record-store

A small TypeScript ESM module for appending and retrieving opaque records in order.

The package is deliberately domain-neutral. It knows about streams, records, and storage lifecycle; it does not know about users, buddies, messages, conversations, or any host application.

## Design status

The durable design is a **working design, not a frozen contract**. The package now
includes both the memory adapter used by fast tests and the segmented local/S3
adapter described below. Memory-store records disappear when the process exits;
do not deploy that adapter as production persistence.

The durable adapter uses local working segments as a write-back cache over one S3 segment namespace:

```text
append opaque record
  -> append complete framed record to local working segment
  -> durably flush local file
  -> acknowledge caller
  -> overwrite that segment's S3 object after at most about five minutes
  -> rotate near 256 KiB; after 14 idle days evict a clean local copy
```

S3 is never on the synchronous append path. The design accepts up to roughly five minutes of loss only when the local durable volume itself is catastrophically lost.

## Public API

The current interface provides:

- `append({ streamId, data })`
- `read({ streamId, beforeId?, afterId?, limit })`
- `getLatest(streamIds)`
- `compareRecordIds(left, right)` and explicit async `close()`
- opaque string stream IDs, record IDs, and record data

The old caller-supplied idempotency key, duplicate result, and stream-deletion API
have been removed. The store does not permanently deduplicate caller requests. A
consuming application may perform cheap, domain-aware best-effort duplicate
suppression at the stream head.

The durable design uses per-record UUIDv7 values internally. Its public 43-character base64url cursor contains both the record UUID and its segment UUID, allowing exact segment lookup without a manifest. Both adapters use this format. Consumers treat it as opaque and call `compareRecordIds()` only when record ordering is actually required. The async durable factory reconstructs local files only and performs no S3 availability check at startup.

## Segment lifecycle summary

- The newest segment is mutable; older segments become immutable when rotation creates a newer segment.
- Before appending, rotate when the current segment is approximately 256 KiB or larger; overshoot by one complete record is fine.
- The first append that makes a clean local segment dirty sets a five-minute checkpoint deadline. Later appends do not move it.
- A durable per-file clean/dirty bit lets startup recover unsynced-to-S3 local bytes without a persisted catalog or remote active namespace.
- If no local file exists, locate the newest S3 segment with one newest-first listing. Download it when it is below the rotation target; otherwise create a new segment.
- After 14 days without a successful append, evict the normally clean local copy. A dirty-and-idle file is an exceptional long-outage recovery case and must upload successfully before eviction. This is cache eviction, not sealing.
- Startup validates local files, reconstructs only the local working set, and queues dirty uploads. It performs no S3 probe, listing, or download.
- Nonlocal segments are fetched as complete objects and retained in a disposable 64-segment in-memory LRU, avoiding repeat `GetObject` calls during continued pagination.
- The MVP assumes exactly one store/service instance.
- The bucket has versioning disabled and never transitions segment objects to Glacier or another restore-required storage class.
- Version 1 uses a small custom big-endian binary frame with leading/trailing lengths and CRC32C, plus SHA-256 for each S3 upload; it does not use Protobuf.

See [docs/DESIGN.md](docs/DESIGN.md) for the complete recovery cases, decisions, open questions, and implementation sequence.

## Usage

The memory adapter is created with:

```ts
import { createMemoryOrderedRecordStore } from '@lsdsoftware/ordered-record-store'

const store = createMemoryOrderedRecordStore()
```

The durable adapter is opened asynchronously with an explicit persistent local
directory and an injected object-store boundary:

```ts
import {
  createS3SegmentObjectStore,
  openSegmentedOrderedRecordStore,
} from '@lsdsoftware/ordered-record-store'
import { S3Client } from '@aws-sdk/client-s3'

const objectStore = createS3SegmentObjectStore({
  client: new S3Client({ region: 'ap-southeast-1' }),
  bucket: 'example-ordered-records',
  keyPrefix: 'messenger',
})

const store = await openSegmentedOrderedRecordStore({
  dataDirectory: '/var/lib/diepkhuc/ordered-record-store',
  objectStore,
})
```

Create one instance per consuming process and inject it into all handlers. The
application owns AWS client configuration and credentials. Call `close()` during
graceful shutdown; dirty local files are recovered and queued on the next start.

## Commands

Node.js 20 or newer is required.

```sh
npm install
npm test
npm run build
```

The package is already consumed by `apsvc-diepkhuc-messenger` through a local `file:` dependency. Publishing or pinning a reproducible package version remains deployment work.
