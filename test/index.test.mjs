import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import {
  createMemoryOrderedRecordStore,
  MAX_DATA_BYTES,
  MAX_IDENTIFIER_BYTES,
  MAX_READ_LIMIT,
  OrderedRecordStoreError,
} from '../dist/index.js'

describe('memory ordered record store', () => {
  test('appends records in order and keeps streams independent', async () => {
    const store = createMemoryOrderedRecordStore()

    const first = await store.append({ streamId: 'one', idempotencyKey: 'a', data: 'first' })
    const other = await store.append({ streamId: 'two', idempotencyKey: 'b', data: 'other' })
    const second = await store.append({ streamId: 'one', idempotencyKey: 'c', data: 'second' })

    assert.equal(first.duplicate, false)
    assert.equal(first.record.id, '1')
    assert.equal(other.record.id, '2')
    assert.equal(second.record.id, '3')
    assert.deepEqual(
      (await store.read({ streamId: 'one', limit: 10 })).map(record => record.data),
      ['first', 'second'],
    )
  })

  test('returns the original record for an idempotent retry', async () => {
    const store = createMemoryOrderedRecordStore()
    const first = await store.append({ streamId: 'one', idempotencyKey: 'request-1', data: 'hello' })
    const retry = await store.append({ streamId: 'one', idempotencyKey: 'request-1', data: 'hello' })

    assert.equal(retry.duplicate, true)
    assert.strictEqual(retry.record, first.record)
    assert.deepEqual(await store.read({ streamId: 'one', limit: 10 }), [first.record])
  })

  test('rejects reuse of an idempotency key with different data', async () => {
    const store = createMemoryOrderedRecordStore()
    await store.append({ streamId: 'one', idempotencyKey: 'request-1', data: 'hello' })

    await assert.rejects(
      store.append({ streamId: 'one', idempotencyKey: 'request-1', data: 'changed' }),
      error => error instanceof OrderedRecordStoreError
        && error.code === 'IDEMPOTENCY_CONFLICT',
    )
  })

  test('reads latest, older, and newer pages in ascending order', async () => {
    const store = createMemoryOrderedRecordStore()
    const records = []
    for (let index = 1; index <= 5; index++) {
      records.push((await store.append({
        streamId: 'one',
        idempotencyKey: String(index),
        data: `record-${index}`,
      })).record)
    }

    assert.deepEqual(
      await store.read({ streamId: 'one', limit: 2 }),
      records.slice(3),
    )
    assert.deepEqual(
      await store.read({ streamId: 'one', beforeId: records[3].id, limit: 2 }),
      records.slice(1, 3),
    )
    assert.deepEqual(
      await store.read({ streamId: 'one', afterId: records[1].id, limit: 2 }),
      records.slice(2, 4),
    )
  })

  test('gets stream heads in one call', async () => {
    const store = createMemoryOrderedRecordStore()
    const first = (await store.append({ streamId: 'one', idempotencyKey: 'a', data: 'first' })).record
    const second = (await store.append({ streamId: 'one', idempotencyKey: 'b', data: 'second' })).record
    const other = (await store.append({ streamId: 'two', idempotencyKey: 'c', data: 'other' })).record

    assert.deepEqual(
      Array.from((await store.getLatest(['one', 'two', 'missing'])).entries()),
      [['one', second], ['two', other], ['missing', null]],
    )
    assert.notStrictEqual(first, second)
  })

  test('deletes and recreates a stream without affecting other streams', async () => {
    const store = createMemoryOrderedRecordStore()
    await store.append({ streamId: 'one', idempotencyKey: 'a', data: 'first' })
    const other = (await store.append({ streamId: 'two', idempotencyKey: 'b', data: 'other' })).record

    await store.deleteStream('one')
    await store.deleteStream('one')
    const recreated = await store.append({ streamId: 'one', idempotencyKey: 'a', data: 'replacement' })

    assert.deepEqual(await store.read({ streamId: 'one', limit: 10 }), [recreated.record])
    assert.deepEqual(await store.read({ streamId: 'two', limit: 10 }), [other])
  })

  test('validates pagination input and public size limits', async () => {
    const store = createMemoryOrderedRecordStore()

    await assert.rejects(
      store.read({ streamId: 'one', beforeId: '1', afterId: '2', limit: 10 }),
      { code: 'INVALID_ARGUMENT' },
    )
    await assert.rejects(
      store.read({ streamId: 'one', limit: 0 }),
      { code: 'INVALID_ARGUMENT' },
    )
    await assert.rejects(
      store.read({ streamId: 'one', limit: MAX_READ_LIMIT + 1 }),
      { code: 'INVALID_ARGUMENT' },
    )
    await assert.rejects(
      store.read({ streamId: 'one', beforeId: 'not-an-id', limit: 10 }),
      { code: 'INVALID_ARGUMENT' },
    )
    await assert.rejects(
      store.append({ streamId: 'x'.repeat(MAX_IDENTIFIER_BYTES + 1), idempotencyKey: 'a', data: '' }),
      { code: 'INVALID_ARGUMENT' },
    )
    await assert.rejects(
      store.append({ streamId: 'one', idempotencyKey: 'a', data: 'x'.repeat(MAX_DATA_BYTES + 1) }),
      { code: 'INVALID_ARGUMENT' },
    )
  })
})
