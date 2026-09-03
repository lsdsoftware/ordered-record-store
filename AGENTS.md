# AGENTS.md

## Project overview

`ordered-record-store` is a TypeScript ESM package containing a domain-neutral interface for ordered opaque records. It was extracted so DiepKhuc Messenger can depend on a storage abstraction without putting Messenger or host-application concepts into storage code.

## Current status

- The public `OrderedRecordStore` interface is defined in `src/index.ts`.
- `createMemoryOrderedRecordStore()` is the only implementation.
- The memory implementation is for Messenger development and tests only; it loses all data on process exit.
- The checked-in memory interface reflects an earlier draft. The working design intentionally removes permanent idempotency and `deleteStream()` before implementing durability.
- The intended durable adapter uses fsynced local active segments, asynchronous S3 active checkpoints, and immutable sealed S3 segments. It does not store one database row per record.

## Commands

```sh
npm install
npm test
npm run build
```

Node.js 20 or newer is required. Create one memory-store instance per consuming process, not one per request or connection.

## Design constraints

- Keep the package independent of Messenger, buddies, users, conversations, and `diepkhuc-web`.
- Store opaque strings. Payload encoding belongs to callers.
- Preserve ordered append and keyset pagination semantics. Do not preserve the old storage-level idempotency API merely for compatibility; the design is deliberately thawed.
- Return record IDs as strings.
- Prefer a small in-process module over a network service at the site's current scale.
- Avoid enterprise infrastructure unless a measured requirement justifies it.
- Do not add S3 to the synchronous append path.
- Do not add a per-stream manifest, persisted runtime catalog, or dirty-marker files without deliberately revising `docs/DESIGN.md`.
- A sealed segment is immutable. A dormant stream starts a new active segment; it never reopens the previous sealed segment.
- Do not log stored data.

Read `docs/DESIGN.md` before changing the public interface or beginning the durable adapter.
