# ordered-record-store

A small TypeScript ESM module for appending and retrieving opaque records in order.

The package is deliberately domain-neutral. It knows about streams, records, and storage lifecycle; it does not know about users, buddies, messages, conversations, or any host application.

## Design status

The durable design is a **working design, not a frozen contract**. The only checked-in implementation is currently memory-only and still exposes the earlier draft API with storage-level idempotency and `deleteStream()`. That code is useful for Messenger development, but it is not the target durable contract and will be changed deliberately.

All memory-store records disappear when the process exits. Do not deploy it as production persistence.

The intended production adapter stores active segments on durable local storage and archives/checkpoints them to S3:

```text
append opaque record
  -> append complete framed record to local active segment
  -> durably flush local file
  -> acknowledge caller
  -> checkpoint dirty active prefix to S3 after at most about five minutes
  -> seal to immutable S3 object at about 256 KiB or after 14 idle days
```

S3 is never on the synchronous append path. The design accepts up to roughly five minutes of loss only when the local durable volume itself is catastrophically lost.

## Target API direction

The next interface revision is expected to retain:

- `append({ streamId, data })`
- `read({ streamId, beforeId?, afterId?, limit })`
- `getLatest(streamIds)`
- `compareRecordIds(left, right)` and explicit async `close()`
- opaque string stream IDs, record IDs, and record data

It is expected to remove:

- caller-supplied `idempotencyKey`
- `AppendResult.duplicate`
- `IDEMPOTENCY_CONFLICT`
- `deleteStream()`

The store does not permanently deduplicate caller requests. A consuming application may perform cheap, domain-aware best-effort duplicate suppression at the stream head. Rare duplicates are preferable here to a durable side index and its recovery rules.

The durable design uses per-record UUIDv7 values internally. Its public 43-character base64url cursor contains both the record UUID and its segment UUID, allowing exact segment lookup without a manifest. Consumers still treat the value as opaque and use `compareRecordIds()` rather than relying on the memory adapter's current decimal IDs. The async durable factory returns only after startup reconciliation; startup fails if S3 cannot be fully reconciled.

## Segment lifecycle summary

- One active segment generation exists only while a stream is active.
- Append complete records and rotate after the resulting file reaches approximately 256 KiB; overshoot by one record is fine.
- Also seal after 14 days without a successful append.
- Sealed segments are immutable.
- A later append to a dormant stream creates a new independently identified active segment. It does not unseal or download the previous segment.
- The first append that makes a clean active segment dirty sets a five-minute checkpoint deadline. Later appends do not move it.
- No per-stream manifest, persisted catalog, or dirty-marker file is maintained.
- Startup rebuilds the in-memory catalog from the complete active frontier, but sealed history remains lazy.
- Normal startup performs local file inspection plus paginated S3 listing and size reconciliation. It does not download every active checkpoint.
- Version 1 uses a small custom big-endian binary frame with leading/trailing lengths and CRC32C, plus SHA-256 for each S3 upload; it does not use Protobuf.

See [docs/DESIGN.md](docs/DESIGN.md) for the complete recovery cases, decisions, open questions, and implementation sequence.

## Current development usage

Until the breaking interface revision lands, the existing memory adapter can still be created with:

```ts
import { createMemoryOrderedRecordStore } from '@lsdsoftware/ordered-record-store'

const store = createMemoryOrderedRecordStore()
```

Create one instance per consuming process and inject it into all handlers. The checked-in TypeScript declarations and tests describe the current mock API; this document and `docs/DESIGN.md` describe the intended next revision.

## Commands

Node.js 20 or newer is required.

```sh
npm install
npm test
npm run build
```

The package is already consumed by `apsvc-diepkhuc-messenger` through a local `file:` dependency. Publishing or pinning a reproducible package version remains deployment work.
