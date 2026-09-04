# AGENTS.md

## Project overview

`ordered-record-store` is a TypeScript ESM package containing a domain-neutral interface for ordered opaque records. It was extracted so DiepKhuc Messenger can depend on a storage abstraction without putting Messenger or host-application concepts into storage code.

## Current status

- Version 0.2.0 implements the current public `OrderedRecordStore` contract.
- `createMemoryOrderedRecordStore()` is the development/test adapter and loses all data on process exit.
- `openSegmentedOrderedRecordStore()` is the durable local/S3 adapter. It fsyncs local segment files before acknowledging appends and checkpoints them asynchronously to S3.
- Permanent storage idempotency and `deleteStream()` are intentionally absent. Domain-specific retry handling belongs to callers.
- The package has 19 contract, S3 mapping, and durability tests. Run `npm test` after changing behavior or framing.

## Commands

```sh
npm install
npm test
npm run build
```

Node.js 20 or newer is required. Create one store instance per consuming process, not one per request or connection.

## Design constraints

- Keep the package independent of Messenger, buddies, users, conversations, and `diepkhuc-web`.
- Store opaque strings. Payload encoding belongs to callers.
- Preserve ordered append and keyset pagination semantics. Do not preserve the old storage-level idempotency API merely for compatibility; the design is deliberately thawed.
- Return record IDs as strings.
- Prefer a small in-process module over a network service at the site's current scale.
- Avoid enterprise infrastructure unless a measured requirement justifies it.
- Do not add S3 to the synchronous append path.
- Do not add a per-stream manifest or persisted runtime catalog. The per-file durable clean/dirty state documented in `docs/DESIGN.md` is required for crash recovery.
- Assume exactly one store/service instance. Multiple writers and distributed ownership are outside the MVP.
- Keep only clean current segments as a local cache; after 14 idle days evict them. Upload dirty files successfully before deleting them.
- Use no remote `active/` namespace. On a local cache miss, lazily locate the newest S3 segment and either download it for continued append or rotate to a new segment.
- Do not log stored data.

Read `docs/DESIGN.md` before changing the public interface or beginning the durable adapter.
