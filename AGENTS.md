# AGENTS.md

## Project overview

`ordered-record-store` is a TypeScript ESM package containing a domain-neutral interface for ordered opaque records. It was extracted so DiepKhuc Messenger can depend on a storage abstraction without putting Messenger or host-application concepts into storage code.

## Current status

- The public `OrderedRecordStore` interface is defined in `src/index.ts`.
- `createMemoryOrderedRecordStore()` is the only implementation.
- The memory implementation is for Messenger development and tests only; it loses all data on process exit.
- The checked-in memory interface reflects an earlier draft. The working design intentionally removes permanent idempotency and `deleteStream()` before implementing durability.
- The intended durable adapter uses fsynced local working segments and asynchronously overwrites the newest segment in one S3 `segments/v1/` namespace. Older segments become immutable when rotation creates a newer one. It does not store one database row per record.

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
- Do not add a per-stream manifest or persisted runtime catalog. The per-file durable clean/dirty state documented in `docs/DESIGN.md` is required for crash recovery.
- Assume exactly one store/service instance. Multiple writers and distributed ownership are outside the MVP.
- Keep only clean current segments as a local cache; after 14 idle days evict them. Upload dirty files successfully before deleting them.
- Use no remote `active/` namespace. On a local cache miss, lazily locate the newest S3 segment and either download it for continued append or rotate to a new segment.
- Do not log stored data.

Read `docs/DESIGN.md` before changing the public interface or beginning the durable adapter.
