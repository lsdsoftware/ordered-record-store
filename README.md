# ordered-record-store

A small TypeScript ESM module for appending and retrieving opaque records in order.

The package is intentionally domain-neutral. It knows about streams and records; it does not know about users, buddies, messages, conversations, or any host application.

## Current status

The only implementation is memory-only. It is intended to unblock development and tests in `apsvc-diepkhuc-messenger` while the durable implementation is deferred.

**All records disappear when the process exits. Do not use the memory store as production persistence.**

## Usage

```ts
import { createMemoryOrderedRecordStore } from '@lsdsoftware/ordered-record-store'

const store = createMemoryOrderedRecordStore()

const { record, duplicate } = await store.append({
  streamId: 'conversation-018f4f4e',
  idempotencyKey: 'client-request-4e7e',
  data: JSON.stringify({ version: 1, senderId: '42', text: 'Hello' }),
})

const latestMessages = await store.read({
  streamId: record.streamId,
  limit: 50,
})
```

Create one store instance when the Messenger process starts and share it across all request handlers and client connections. Creating a store per request would create isolated histories.

## API behavior

- `append()` assigns a globally increasing record ID represented as a string.
- Records in one stream are ordered by that ID.
- Retrying an append with the same stream, idempotency key, and data returns the original record with `duplicate: true`.
- Reusing an idempotency key with different data throws `IDEMPOTENCY_CONFLICT`.
- `read()` uses record IDs for keyset pagination and always returns results in ascending order.
- With neither `beforeId` nor `afterId`, `read()` returns the newest page.
- `beforeId` loads an older page; `afterId` loads records for forward catch-up.
- `getLatest()` retrieves the current head of several streams in one call.
- `deleteStream()` is idempotent.
- Deleting a stream also clears its idempotency history. The same stream ID and idempotency keys may be used again afterward.

Record IDs are decimal strings in the current implementation, but consumers should treat them as opaque values and must not convert them to JavaScript numbers.

Public limits are exported as constants:

- Stream IDs and idempotency keys: at most 64 UTF-8 bytes
- Record data: at most 65,535 UTF-8 bytes
- Read limit: at most 1,000 records

`createdAt` is a UTC ISO-8601 timestamp with millisecond precision.

For older pagination, pass the first returned record's ID as the next `beforeId`. For forward catch-up, pass the last returned record's ID as `afterId`. Fewer records than the requested limit means that direction is currently exhausted. Boundary IDs are numeric ordering boundaries and do not need to exist in the requested stream.

Invalid requests and idempotency conflicts throw `OrderedRecordStoreError`. Consumers can inspect its `code`, currently `INVALID_ARGUMENT` or `IDEMPOTENCY_CONFLICT`.

## Commands

Node.js 20 or newer is required.

```sh
npm install
npm test
npm run build
```

The package has not yet been wired into `apsvc-diepkhuc-messenger` or published. A local `file:` dependency versus publishing to npm remains an integration decision. The `prepare` script builds `dist` when installing directly from a Git or local package source.

See [docs/DESIGN.md](docs/DESIGN.md) for project context, design boundaries, Messenger integration, and the deferred durable implementation.
