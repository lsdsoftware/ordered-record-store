const TABLE = createTable()

/** CRC-32C (Castagnoli), returned as an unsigned 32-bit integer. */
export function crc32c(data: Uint8Array): number {
  let crc = 0xffffffff
  for (const byte of data) {
    crc = TABLE[(crc ^ byte) & 0xff]! ^ (crc >>> 8)
  }
  return (crc ^ 0xffffffff) >>> 0
}

function createTable(): Uint32Array {
  const table = new Uint32Array(256)
  for (let index = 0; index < table.length; index++) {
    let value = index
    for (let bit = 0; bit < 8; bit++) {
      value = (value & 1) === 1
        ? 0x82f63b78 ^ (value >>> 1)
        : value >>> 1
    }
    table[index] = value >>> 0
  }
  return table
}
