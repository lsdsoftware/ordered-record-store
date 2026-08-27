# Design and continuation notes

Last updated: 2026-08-27.

## Why this repository exists

DiepKhuc needs persistent one-to-one messages, but persistence should not depend on Messenger domain concepts or on the structure of `diepkhuc-web`. This package provides that boundary as an ordered store of opaque string records.

The site has a few thousand users and is operated by one person. The design goal is therefore modest reliability with low implementation and operational complexity—not enterprise-scale availability.

## Current decision

Use this package directly as a TypeScript ESM dependency of `apsvc-diepkhuc-messenger`.

For now, inject `createMemoryOrderedRecordStore()` into Messenger. This lets Messenger implement its domain behavior before durable storage is ready. The memory store is disposable and loses all records on restart.

Create exactly one memory-store instance at Messenger process startup. Share that instance across request handlers and client connections.

Do not turn this package into a service-broker service yet. A network service would add authentication, deployment, and partial-failure concerns without improving the code boundary for the current scale.

## Boundary

This package owns:

- Appending opaque strings to opaque streams
- Assigning stable ordered record IDs
- Idempotent retry behavior
- Keyset pagination
- Reading the latest record for a set of streams
- Whole-stream deletion

Messenger owns:

- Mapping a two-account conversation to an opaque stream ID
- Buddy requests and accepted relationships
- Authorization to send or read
- Payload schema and JSON serialization
- Message length and abuse limits
- Online presence and live client notification
- Conversation summaries and per-user read state
- User-facing deletion meaning

The host application must call Messenger, never this store directly.

The contract currently limits stream IDs and idempotency keys to 64 UTF-8 bytes, record data to 65,535 UTF-8 bytes, and page reads to 1,000 records. Every adapter must enforce the same limits.

`createdAt` is canonical UTC in ISO-8601 format with millisecond precision. A MySQL implementation must configure its connection/session timezone as UTC.

`deleteStream()` physically removes the stream's records and idempotency history. A later append recreates the stream, and previously used idempotency keys may be used again.

## Messenger integration

Use an opaque conversation ID as `streamId`; do not expose a pair such as `123-456` in storage keys.

Messenger can encode a record like this:

```json
{
  "version": 1,
  "type": "message",
  "senderId": "123",
  "text": "Hello"
}
```

Suggested send flow:

1. Authenticate the client and verify the accepted buddy relationship.
2. Resolve or create the conversation and its opaque stream ID.
3. Call `append()` using the client-generated message ID as `idempotencyKey`.
4. Treat the returned record ID and timestamp as canonical.
5. Update Messenger's conversation/read projection.
6. Notify connected clients.

When a durable adapter is implemented, `append()` must resolve only after the database commit. If notifying a client fails after commit, the message remains available through history retrieval.

## Deferred MySQL adapter

The intended first durable implementation is one InnoDB table, not an S3-backed append-file system:

```sql
CREATE TABLE orderedRecord (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  streamId VARBINARY(64) NOT NULL,
  idempotencyKey VARBINARY(64) NOT NULL,
  data TEXT NOT NULL,
  createdAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (id),
  UNIQUE KEY streamId_idempotencyKey (streamId, idempotencyKey),
  KEY streamId_id (streamId, id)
) ENGINE=InnoDB;
```

The MySQL adapter should implement the existing `OrderedRecordStore` interface. Avoid expanding the public contract unless a concrete Messenger requirement cannot be satisfied by it.

Use ordinary MySQL backups to S3. Do not put S3 on the synchronous message-write path.

Binary identifier columns are intentional: the memory adapter compares IDs and idempotency keys by exact string value, so a case-insensitive MySQL collation would violate the contract.

## Durable-adapter checklist

- Preserve current idempotency behavior, including conflict detection.
- Return IDs as strings.
- Commit before resolving `append()`.
- Implement `beforeId` and `afterId` with `(streamId, id)` keyset queries.
- Return read results in ascending order.
- Do not log record data.
- Add a shared contract test suite and run it against both memory and MySQL adapters.
- Test process restart, concurrent append, duplicate retry, and pagination.
- Document backup and restore commands.

## Deliberately deferred

- Multiple storage services or regions
- Redis or distributed coordination
- Guaranteed realtime event delivery
- S3 record archival
- Complex retention classes
- Per-record redaction workflows
- Full-text search
- Transactions spanning multiple streams
- Event sourcing of the Messenger domain

Revisit these only when actual usage or product requirements justify them.

## Open implementation decisions

These have deliberately not been decided yet:

- Whether Messenger consumes this package through a local `file:` dependency or a published npm version
- Whether the MySQL adapter owns its connection pool or accepts an injected pool
- Which maintained MySQL client package to use
- Whether migrations live here or in the deployment/database repository
- How MySQL operational errors map into additional public error codes

Do not interpret these omissions as settled architecture.

## Resume here in a future session

1. Read this file and `README.md`.
2. Run `npm test`.
3. Inspect how `apsvc-diepkhuc-messenger` consumes `OrderedRecordStore`.
4. Confirm the real MySQL version and deployment credentials model.
5. Implement a MySQL adapter behind the existing interface.
6. Run the same behavior tests against both implementations.
