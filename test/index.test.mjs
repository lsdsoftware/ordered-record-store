import assert from 'node:assert/strict'
import { appendFile, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { describe, test } from 'node:test'

import {
  compareRecordIds,
  createMemoryOrderedRecordStore,
  createS3SegmentObjectStore,
  MAX_DATA_BYTES,
  MAX_IDENTIFIER_BYTES,
  MAX_READ_LIMIT,
  openSegmentedOrderedRecordStore,
} from '../dist/index.js'
import { crc32c } from '../dist/crc32c.js'

test('CRC32C matches the published check vector', () => {
  assert.equal(crc32c(Buffer.from('123456789')), 0xe3069283)
})

test('S3 object adapter maps prefixes, complete objects, checksums, and missing keys', async () => {
  const calls = []
  const responses = [
    { Contents: [{ Key: 'messenger/segments/v1/aa/object.ors', Size: 123 }] },
    { Body: { transformToByteArray: async () => Uint8Array.from([1, 2, 3]) } },
    {},
  ]
  const client = {
    async send(command) {
      calls.push(command)
      return responses.shift()
    },
  }
  const objects = createS3SegmentObjectStore({
    client,
    bucket: 'records',
    keyPrefix: 'messenger',
  })

  assert.deepEqual(await objects.list({
    prefix: 'segments/v1/aa/',
    startAfter: 'segments/v1/aa/previous.ors',
    limit: 7,
  }), [{ key: 'segments/v1/aa/object.ors', size: 123 }])
  assert.deepEqual(Array.from(await objects.get('segments/v1/aa/object.ors')), [1, 2, 3])
  await objects.put({
    key: 'segments/v1/aa/object.ors',
    data: Uint8Array.from([4, 5]),
    checksumSha256: 'checksum',
  })

  assert.deepEqual(calls.map(call => call.input), [
    {
      Bucket: 'records',
      Prefix: 'messenger/segments/v1/aa/',
      StartAfter: 'messenger/segments/v1/aa/previous.ors',
      MaxKeys: 7,
    },
    {
      Bucket: 'records',
      Key: 'messenger/segments/v1/aa/object.ors',
    },
    {
      Bucket: 'records',
      Key: 'messenger/segments/v1/aa/object.ors',
      Body: Uint8Array.from([4, 5]),
      ChecksumSHA256: 'checksum',
    },
  ])

  client.send = async () => {
    throw { $metadata: { httpStatusCode: 404 } }
  }
  assert.equal(await objects.get('missing.ors'), null)
})

const contractFactories = [
  ['memory', async () => ({ store: createMemoryOrderedRecordStore(), cleanup: async () => {} })],
  ['segmented', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-contract-'))
    const store = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: new FakeObjectStore(),
      checkpointDelayMilliseconds: 60_000,
    })
    return {
      store,
      cleanup: async () => rm(directory, { recursive: true, force: true }),
    }
  }],
]

for (const [name, create] of contractFactories) {
  describe(`${name} ordered record store contract`, () => {
    test('appends ordered records and keeps streams independent', async () => {
      const { store, cleanup } = await create()
      try {
        const first = await store.append({ streamId: 'one', data: 'first' })
        const other = await store.append({ streamId: 'two', data: 'other' })
        const second = await store.append({ streamId: 'one', data: 'second' })

        assert.match(first.id, /^[A-Za-z0-9_-]{43}$/)
        assert.equal(compareRecordIds(first.id, second.id), -1)
        assert.equal(compareRecordIds(second.id, first.id), 1)
        assert.equal(compareRecordIds(first.id, first.id), 0)
        assert.equal(other.streamId, 'two')
        assert.deepEqual(
          (await store.read({ streamId: 'one', limit: 10 })).map(record => record.data),
          ['first', 'second'],
        )
      } finally {
        await store.close()
        await cleanup()
      }
    })

    test('reads latest, older, and newer pages in ascending order', async () => {
      const { store, cleanup } = await create()
      try {
        const records = []
        for (let index = 1; index <= 5; index++) {
          records.push(await store.append({ streamId: 'one', data: `record-${index}` }))
        }
        assert.deepEqual(await store.read({ streamId: 'one', limit: 2 }), records.slice(3))
        assert.deepEqual(
          await store.read({ streamId: 'one', beforeId: records[3].id, limit: 2 }),
          records.slice(1, 3),
        )
        assert.deepEqual(
          await store.read({ streamId: 'one', afterId: records[1].id, limit: 2 }),
          records.slice(2, 4),
        )
        await assert.rejects(
          store.read({ streamId: 'two', beforeId: records[0].id, limit: 1 }),
          { code: 'INVALID_ARGUMENT' },
        )
      } finally {
        await store.close()
        await cleanup()
      }
    })

    test('gets stream heads and validates public inputs', async () => {
      const { store, cleanup } = await create()
      try {
        const first = await store.append({ streamId: 'one', data: 'first' })
        const second = await store.append({ streamId: 'one', data: 'second' })
        const other = await store.append({ streamId: 'two', data: 'other' })
        assert.deepEqual(
          Array.from((await store.getLatest(['one', 'two', 'missing'])).entries()),
          [['one', second], ['two', other], ['missing', null]],
        )
        assert.notStrictEqual(first, second)

        await assert.rejects(
          store.read({ streamId: 'one', beforeId: first.id, afterId: second.id, limit: 10 }),
          { code: 'INVALID_ARGUMENT' },
        )
        await assert.rejects(store.read({ streamId: 'one', limit: 0 }), { code: 'INVALID_ARGUMENT' })
        await assert.rejects(
          store.read({ streamId: 'one', limit: MAX_READ_LIMIT + 1 }),
          { code: 'INVALID_ARGUMENT' },
        )
        await assert.rejects(
          store.read({ streamId: 'one', beforeId: 'not-an-id', limit: 10 }),
          { code: 'INVALID_ARGUMENT' },
        )
        await assert.rejects(
          store.append({ streamId: 'x'.repeat(MAX_IDENTIFIER_BYTES + 1), data: '' }),
          { code: 'INVALID_ARGUMENT' },
        )
        await assert.rejects(
          store.append({ streamId: 'one', data: 'x'.repeat(MAX_DATA_BYTES + 1) }),
          { code: 'INVALID_ARGUMENT' },
        )
      } finally {
        await store.close()
        await cleanup()
      }
    })

    test('rejects work after close and permits repeated close', async () => {
      const { store, cleanup } = await create()
      try {
        await store.close()
        await store.close()
        await assert.rejects(store.append({ streamId: 'one', data: 'x' }), { code: 'CLOSED' })
        await assert.rejects(store.read({ streamId: 'one', limit: 1 }), { code: 'CLOSED' })
        await assert.rejects(store.getLatest(['one']), { code: 'CLOSED' })
      } finally {
        await cleanup()
      }
    })
  })
}

describe('segmented ordered record store durability', () => {
  test('recovers a locally durable dirty segment without contacting object storage at startup', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-restart-'))
    const objects = new FakeObjectStore()
    const firstStore = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 60_000,
    })
    const first = await firstStore.append({ streamId: 'one', data: 'first' })
    await firstStore.close()
    objects.listCalls = 0
    objects.getCalls = []
    objects.failAll = true

    const secondStore = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 60_000,
      retryDelayMilliseconds: 20,
    })
    try {
      assert.equal(objects.listCalls, 0)
      assert.equal(objects.getCalls.length, 0)
      assert.deepEqual(await secondStore.read({ streamId: 'one', limit: 1 }), [first])
      const second = await secondStore.append({ streamId: 'one', data: 'second' })
      assert.equal(second.data, 'second')
    } finally {
      await secondStore.close()
      await rm(directory, { recursive: true, force: true })
    }
  })

  test('truncates only an incomplete dirty tail during restart', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-tail-'))
    const objects = new FakeObjectStore()
    const store = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 60_000,
    })
    const record = await store.append({ streamId: 'one', data: 'complete' })
    await store.close()
    const [dirtyFile] = await findFiles(directory, '.dirty.ors')
    assert.ok(dirtyFile)
    const before = (await readFile(dirtyFile)).length
    await appendFile(dirtyFile, Buffer.from([0, 0, 0, 50, 1, 2, 3]))

    const reopened = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 60_000,
    })
    try {
      assert.equal((await readFile(dirtyFile)).length, before)
      assert.deepEqual(await reopened.read({ streamId: 'one', limit: 10 }), [record])
    } finally {
      await reopened.close()
      await rm(directory, { recursive: true, force: true })
    }
  })

  test('uses audit-time checkpointing and uploads the captured complete prefix', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-audit-'))
    const objects = new FakeObjectStore()
    const store = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 100,
      retryDelayMilliseconds: 20,
    })
    try {
      await store.append({ streamId: 'one', data: 'first' })
      await delay(60)
      await store.append({ streamId: 'one', data: 'second' })
      await delay(55)
      assert.equal(objects.puts.length, 1, 'second append must not move the first deadline')
      await waitFor(() => objects.puts.length >= 1)
      const uploaded = objects.puts[0].data
      assert.ok(uploaded.includes(Buffer.from('first')))
      assert.ok(uploaded.includes(Buffer.from('second')))
    } finally {
      await store.close()
      await rm(directory, { recursive: true, force: true })
    }
  })

  test('publishes rotated segments oldest first and reads across them', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-rotate-'))
    const objects = new FakeObjectStore()
    const store = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      rotationTargetBytes: 120,
      checkpointDelayMilliseconds: 20,
      retryDelayMilliseconds: 20,
    })
    try {
      const first = await store.append({ streamId: 'one', data: 'x'.repeat(80) })
      const second = await store.append({ streamId: 'one', data: 'second' })
      const third = await store.append({ streamId: 'one', data: 'third' })
      await waitFor(() => objects.puts.length >= 2)
      assert.notEqual(objects.puts[0].key, objects.puts[1].key)
      assert.ok(objects.puts[0].key > objects.puts[1].key, 'older inverted key sorts after newer')
      assert.deepEqual(await store.read({ streamId: 'one', limit: 10 }), [first, second, third])
      assert.deepEqual(
        await store.read({ streamId: 'one', afterId: first.id, limit: 10 }),
        [second, third],
      )
      assert.deepEqual(
        await store.read({ streamId: 'one', beforeId: third.id, limit: 10 }),
        [first, second],
      )
    } finally {
      await store.close()
      await rm(directory, { recursive: true, force: true })
    }
  })

  test('rehydrates a below-target newest segment and caches a complete nonlocal head', async () => {
    const firstDirectory = await mkdtemp(path.join(tmpdir(), 'ors-source-'))
    const secondDirectory = await mkdtemp(path.join(tmpdir(), 'ors-cache-'))
    const thirdDirectory = await mkdtemp(path.join(tmpdir(), 'ors-reader-'))
    const objects = new FakeObjectStore()
    const source = await openSegmentedOrderedRecordStore({
      dataDirectory: firstDirectory,
      objectStore: objects,
      checkpointDelayMilliseconds: 20,
    })
    const first = await source.append({ streamId: 'one', data: 'first' })
    await waitFor(() => objects.puts.length >= 1)
    await source.close()

    const reader = await openSegmentedOrderedRecordStore({
      dataDirectory: thirdDirectory,
      objectStore: objects,
      checkpointDelayMilliseconds: 20,
    })
    assert.deepEqual(await reader.getLatest(['one']), new Map([['one', first]]))
    const getsAfterHead = objects.getCalls.length
    assert.deepEqual(await reader.read({ streamId: 'one', limit: 10 }), [first])
    assert.equal(objects.getCalls.length, getsAfterHead, 'history reuses the complete cached segment')
    await reader.close()

    const continuation = await openSegmentedOrderedRecordStore({
      dataDirectory: secondDirectory,
      objectStore: objects,
      checkpointDelayMilliseconds: 20,
    })
    try {
      const second = await continuation.append({ streamId: 'one', data: 'second' })
      assert.equal(first.id.slice(22), second.id.slice(22), 'append continues the downloaded segment')
      assert.deepEqual(await continuation.read({ streamId: 'one', limit: 10 }), [first, second])
    } finally {
      await continuation.close()
      await Promise.all([firstDirectory, secondDirectory, thirdDirectory].map(
        directory => rm(directory, { recursive: true, force: true }),
      ))
    }
  })

  test('retains dirty data through a failed checkpoint and retries', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-retry-'))
    const objects = new FakeObjectStore()
    objects.failPuts = 1
    const store = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 10,
      retryDelayMilliseconds: 20,
    })
    try {
      await store.append({ streamId: 'one', data: 'survives' })
      await waitFor(() => objects.putAttempts >= 2)
      assert.equal(objects.puts.length, 1)
      assert.equal((await findFiles(directory, '.clean.ors')).length, 1)
    } finally {
      await store.close()
      await rm(directory, { recursive: true, force: true })
    }
  })

  test('an append during upload gets a later checkpoint without regressing the object', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-overlap-'))
    const objects = new FakeObjectStore()
    const blockedPut = objects.holdNextPut()
    const store = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 20,
      retryDelayMilliseconds: 20,
    })
    try {
      await store.append({ streamId: 'one', data: 'first' })
      await blockedPut.entered
      await store.append({ streamId: 'one', data: 'second' })
      blockedPut.release()
      await waitFor(() => objects.puts.length >= 2)

      assert.ok(objects.puts[0].data.includes(Buffer.from('first')))
      assert.equal(objects.puts[0].data.includes(Buffer.from('second')), false)
      assert.ok(objects.puts[1].data.includes(Buffer.from('second')))
      assert.ok(objects.puts[1].data.length > objects.puts[0].data.length)
    } finally {
      await store.close()
      await rm(directory, { recursive: true, force: true })
    }
  })

  test('evicts an idle clean cache and lazily rehydrates it for a later append', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-idle-'))
    const objects = new FakeObjectStore()
    const store = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 10,
      idleEvictionMilliseconds: 60,
      sweepIntervalMilliseconds: 10,
    })
    try {
      const first = await store.append({ streamId: 'one', data: 'first' })
      await waitFor(() => objects.puts.length >= 1)
      await waitFor(async () =>
        (await findFiles(directory, '.clean.ors')).length === 0
        && (await findFiles(directory, '.dirty.ors')).length === 0,
      2_000)
      assert.deepEqual(await store.getLatest(['one']), new Map([['one', first]]))

      const second = await store.append({ streamId: 'one', data: 'second' })
      assert.equal(first.id.slice(22), second.id.slice(22))
      assert.deepEqual(await store.read({ streamId: 'one', limit: 2 }), [first, second])
    } finally {
      await store.close()
      await rm(directory, { recursive: true, force: true })
    }
  })

  test('quarantines a corrupt clean cache instead of uploading it over S3', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-clean-corrupt-'))
    const objects = new FakeObjectStore()
    const store = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 10,
    })
    const record = await store.append({ streamId: 'one', data: 'authoritative' })
    await waitFor(async () => (await findFiles(directory, '.clean.ors')).length === 1)
    await store.close()

    const [cleanFile] = await findFiles(directory, '.clean.ors')
    const corruptBytes = await readFile(cleanFile)
    corruptBytes[corruptBytes.length - 9] ^= 0xff
    await writeFile(cleanFile, corruptBytes)
    const putsBeforeRestart = objects.puts.length

    const reopened = await openSegmentedOrderedRecordStore({
      dataDirectory: directory,
      objectStore: objects,
      checkpointDelayMilliseconds: 10,
    })
    try {
      assert.equal(objects.puts.length, putsBeforeRestart)
      assert.equal((await findFiles(directory, '.clean.ors')).length, 0)
      assert.deepEqual(await reopened.getLatest(['one']), new Map([['one', record]]))
    } finally {
      await reopened.close()
      await rm(directory, { recursive: true, force: true })
    }
  })
})

class FakeObjectStore {
  objects = new Map()
  puts = []
  putAttempts = 0
  failPuts = 0
  failAll = false
  listCalls = 0
  getCalls = []
  #putHolds = []

  holdNextPut() {
    const entered = deferred()
    const released = deferred()
    this.#putHolds.push({ entered, released })
    return { entered: entered.promise, release: () => released.resolve() }
  }

  async list({ prefix, startAfter, limit }) {
    this.listCalls++
    if (this.failAll) throw new Error('object store unavailable')
    return Array.from(this.objects, ([key, data]) => ({ key, size: data.length }))
      .filter(object => object.key.startsWith(prefix))
      .filter(object => startAfter === undefined || object.key > startAfter)
      .sort((left, right) => left.key.localeCompare(right.key))
      .slice(0, limit)
  }

  async get(key) {
    this.getCalls.push({ key })
    if (this.failAll) throw new Error('object store unavailable')
    const data = this.objects.get(key)
    if (data === undefined) return null
    return Buffer.from(data)
  }

  async put({ key, data, checksumSha256 }) {
    this.putAttempts++
    if (this.failAll || this.failPuts-- > 0) throw new Error('put failed')
    const hold = this.#putHolds.shift()
    if (hold) {
      hold.entered.resolve()
      await hold.released.promise
    }
    const copy = Buffer.from(data)
    this.objects.set(key, copy)
    this.puts.push({ key, data: copy, checksumSha256 })
  }
}

async function findFiles(directory, suffix) {
  const result = []
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const child = path.join(directory, entry.name)
    if (entry.isDirectory()) result.push(...await findFiles(child, suffix))
    else if (entry.name.endsWith(suffix)) result.push(child)
  }
  return result
}

async function waitFor(predicate, timeoutMilliseconds = 2_000) {
  const deadline = Date.now() + timeoutMilliseconds
  while (!await predicate()) {
    if (Date.now() >= deadline) throw new Error('timed out waiting for condition')
    await delay(10)
  }
}

function deferred() {
  let resolve
  let reject
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

function delay(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}

describe('bounded discovery metadata', () => {
  async function open(t, objects = new FakeObjectStore(), options = {}) {
    const directory = await mkdtemp(path.join(tmpdir(), 'ors-discovery-'))
    const store = await openSegmentedOrderedRecordStore({
      dataDirectory: directory, objectStore: objects,
      checkpointDelayMilliseconds: 60_000, ...options,
    })
    t.after(async () => { await store.close(); await rm(directory, {recursive: true, force: true}) })
    return {store, objects, directory}
  }

  test('shares absent heads across latest/read/append and knows both ends of new streams', async t => {
    const {store, objects} = await open(t)
    assert.equal((await store.getLatest(['s'])).get('s'), null)
    for (let i = 0; i < 3; i++) assert.deepEqual(await store.read({streamId: 's', limit: 50}), [])
    const first = await store.append({streamId: 's', data: 'first'})
    const second = await store.append({streamId: 's', data: 'second'})
    assert.equal(objects.listCalls, 1)
    for (let i = 0; i < 3; i++) {
      assert.deepEqual(await store.read({streamId: 's', afterId: second.id, limit: 1000}), [])
      assert.deepEqual(await store.read({streamId: 's', afterId: first.id, limit: 1000}), [second])
      assert.deepEqual(await store.read({streamId: 's', beforeId: first.id, limit: 1}), [])
      assert.deepEqual(await store.read({streamId: 's', limit: 50}), [first, second])
    }
    assert.equal(objects.listCalls, 1)
    const foreign = await store.append({streamId: 'other', data: 'foreign'})
    await assert.rejects(store.read({streamId: 's', afterId: foreign.id, limit: 1}), error => error.code === 'INVALID_ARGUMENT')
  })

  test('shares remote head discovery with reads and append, and retains the oldest boundary', async t => {
    const objects = new FakeObjectStore()
    const [first] = await seedSegments(objects, 's', 1)
    const {store} = await open(t, objects)
    assert.equal((await store.getLatest(['s'])).get('s').id, first.id)
    for (let i = 0; i < 3; i++) {
      assert.equal((await store.read({streamId: 's', limit: 1}))[0].id, first.id)
      assert.deepEqual(await store.read({streamId: 's', afterId: first.id, limit: 1000}), [])
    }
    assert.equal(objects.listCalls, 1)
    await store.read({streamId: 's', limit: 50})
    assert.equal(objects.listCalls, 2, 'first oldest-boundary discovery')
    for (let i = 0; i < 3; i++) await store.read({streamId: 's', limit: 50})
    const second = await store.append({streamId: 's', data: 'second'})
    assert.equal(objects.listCalls, 2)
    assert.equal((await store.getLatest(['s'])).get('s').id, second.id)
  })

  test('one page supplies reusable remote links in both directions', async t => {
    const objects = new FakeObjectStore()
    const records = await seedSegments(objects, 's', 3)
    const {store} = await open(t, objects)
    for (let i = 0; i < 3; i++) {
      const newer = await store.read({streamId: 's', afterId: records[0].id, limit: 1000})
      assert.deepEqual(newer.map(r => r.id), records.slice(1).map(r => r.id))
      const all = await store.read({streamId: 's', limit: 1000})
      assert.deepEqual(all.map(r => r.id), records.map(r => r.id))
    }
    assert.equal(objects.listCalls, 1)
  })

  test('retains links across listing pages without assuming arbitrary cached segments are adjacent', async t => {
    const objects = new FakeObjectStore()
    const records = await seedSegments(objects, 's', 1002)
    const {store} = await open(t, objects)
    const result = await store.read({streamId: 's', afterId: records[0].id, limit: 1000})
    assert.deepEqual(result.map(r => r.id), records.slice(1, 1001).map(r => r.id))
    assert.equal(objects.listCalls, 2)
    const repeated = await store.read({streamId: 's', afterId: records[0].id, limit: 1000})
    assert.deepEqual(repeated.map(r => r.id), result.map(r => r.id))
    assert.deepEqual((await store.read({streamId: 's', afterId: records[1000].id, limit: 1000})).map(r => r.id), [records[1001].id])
    assert.equal(objects.listCalls, 2)
  })

  test('head eviction rediscovers safely and failed discovery does not cache absence', async t => {
    const {store, objects} = await open(t)
    await store.getLatest(['first'])
    for (let i = 0; i < 1024; i++) await store.getLatest([`empty-${i}`])
    const before = objects.listCalls
    await store.getLatest(['first'])
    assert.equal(objects.listCalls, before + 1)
    objects.failAll = true
    await assert.rejects(store.getLatest(['failure']))
    objects.failAll = false
    const count = objects.listCalls
    await store.getLatest(['failure'])
    assert.equal(objects.listCalls, count + 1)
  })

  test('head records survive remote byte-cache eviction without introducing extra GETs', async t => {
    const objects = new FakeObjectStore()
    for (let i = 0; i < 70; i++) await seedSegments(objects, `s-${i}`, 1)
    const {store} = await open(t, objects)
    for (let i = 0; i < 70; i++) await store.getLatest([`s-${i}`])
    const gets = objects.getCalls.length
    const lists = objects.listCalls
    await store.getLatest(['s-0'])
    assert.equal(objects.getCalls.length, gets)
    assert.equal(objects.listCalls, lists)
  })

  test('failed append rehydration invalidates head assumptions and retries safely', async t => {
    const objects = new FakeObjectStore()
    const [original] = await seedSegments(objects, 'original', 1)
    for (let i = 0; i < 70; i++) await seedSegments(objects, `other-${i}`, 1)
    const {store} = await open(t, objects)
    await store.getLatest(['original'])
    // Evict the original bytes while retaining its head metadata.
    for (let i = 0; i < 70; i++) await store.getLatest([`other-${i}`])
    objects.failAll = true
    await assert.rejects(store.append({streamId: 'original', data: 'failed'}))
    objects.failAll = false
    const lists = objects.listCalls
    const appended = await store.append({streamId: 'original', data: 'accepted'})
    assert.equal(objects.listCalls, lists + 1)
    assert.deepEqual((await store.read({streamId: 'original', limit: 2})).map(r => r.id), [original.id, appended.id])
  })

  test('evicted segment links are safely rediscovered', async t => {
    const objects = new FakeObjectStore()
    const streams = []
    for (let i = 0; i < 5; i++) streams.push(await seedSegments(objects, `s-${i}`, 1001))
    const {store} = await open(t, objects)
    for (let i = 0; i < 5; i++) {
      const page = await store.read({streamId: `s-${i}`, beforeId: streams[i].at(-1).id, limit: 1})
      assert.equal(page[0].id, streams[i].at(-2).id)
    }
    assert.equal(objects.listCalls, 5)
    const repeated = await store.read({streamId: 's-0', beforeId: streams[0].at(-1).id, limit: 1})
    assert.equal(repeated[0].id, streams[0].at(-2).id)
    assert.equal(objects.listCalls, 6)
  })

  test('local rotation needs no discovery while prior publication is blocked', async t => {
    const objects = new FakeObjectStore()
    const hold = objects.holdNextPut()
    const {store} = await open(t, objects, {rotationTargetBytes: 1, checkpointDelayMilliseconds: 0})
    const first = await store.append({streamId: 's', data: 'first'})
    await hold.entered
    try {
      const second = await store.append({streamId: 's', data: 'second'})
      const third = await store.append({streamId: 's', data: 'third'})
      assert.deepEqual(await store.read({streamId: 's', limit: 50}), [first, second, third])
      assert.deepEqual(await store.read({streamId: 's', afterId: first.id, limit: 50}), [second, third])
      assert.equal(objects.listCalls, 1)
    } finally { hold.release() }
  })

  test('rebuilds discovery after restart and merges unpublished local head with remote history', async t => {
    const objects = new FakeObjectStore()
    const old = await seedSegments(objects, 's', 3)
    const {store, directory} = await open(t, objects, {rotationTargetBytes: 1})
    const newest = await store.append({streamId: 's', data: 'not published'})
    await store.close()
    const hold = objects.holdNextPut()
    const reopened = await openSegmentedOrderedRecordStore({dataDirectory: directory, objectStore: objects})
    try {
      await hold.entered
      const start = objects.listCalls
      assert.equal((await reopened.getLatest(['s'])).get('s').id, newest.id)
      assert.equal(objects.listCalls, start, 'startup and local head need no discovery')
      const read = await reopened.read({streamId: 's', afterId: old[0].id, limit: 50})
      assert.deepEqual(read.map(r => r.id), [old[1].id, old[2].id, newest.id])
      assert.equal(objects.listCalls, start + 1)
      await reopened.read({streamId: 's', afterId: old[0].id, limit: 50})
      assert.equal(objects.listCalls, start + 1)
    } finally { hold.release(); await reopened.close() }
  })
})

async function seedSegments(objects, streamId, count) {
  const {createHash} = await import('node:crypto')
  const {generateRecordIdentity, invertUuidHex} = await import('../dist/record-id.js')
  const {encodeSegmentHeader, encodeRecordFrame} = await import('../dist/segment-codec.js')
  const hash = createHash('sha256').update(streamId).digest('hex')
  const records = []
  for (let i = 0; i < count; i++) {
    const identity = generateRecordIdentity()
    const data = `record-${i}`
    const key = `segments/v1/${hash.slice(0, 2)}/${hash.slice(2)}/${invertUuidHex(identity.segmentBytes)}-${identity.segmentHex}.ors`
    objects.objects.set(key, Buffer.concat([
      encodeSegmentHeader(streamId, identity.segmentBytes, identity.createdAtMilliseconds),
      encodeRecordFrame(identity, data),
    ]))
    records.push({id: identity.id, streamId, data, createdAt: new Date(identity.createdAtMilliseconds).toISOString()})
  }
  return records
}
