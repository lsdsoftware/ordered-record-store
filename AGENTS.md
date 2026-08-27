# AGENTS.md

## Project overview

`ordered-record-store` is a TypeScript ESM package containing a domain-neutral interface for ordered opaque records. It was extracted so DiepKhuc Messenger can depend on a storage abstraction without putting Messenger or host-application concepts into storage code.

## Current status

- The public `OrderedRecordStore` interface is defined in `src/index.ts`.
- `createMemoryOrderedRecordStore()` is the only implementation.
- The memory implementation is for Messenger development and tests only; it loses all data on process exit.
- A simple MySQL/InnoDB adapter is intentionally deferred.

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
- Preserve idempotent append behavior and keyset pagination semantics.
- Return record IDs as strings.
- Prefer a small in-process module over a network service at the site's current scale.
- Avoid enterprise infrastructure unless a measured requirement justifies it.
- Do not add S3 to the synchronous append path.
- Do not log stored data.

Read `docs/DESIGN.md` before changing the public interface or beginning the durable adapter.
