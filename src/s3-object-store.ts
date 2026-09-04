import {
  GetObjectCommand,
  ListObjectsV2Command,
  NoSuchKey,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3'

import type { SegmentObjectStore } from './types.js'

export interface S3SegmentObjectStoreOptions {
  readonly client: S3Client
  readonly bucket: string
  readonly keyPrefix?: string
}

export function createS3SegmentObjectStore(
  options: S3SegmentObjectStoreOptions,
): SegmentObjectStore {
  const keyPrefix = normalizePrefix(options.keyPrefix ?? '')
  return {
    async list({ prefix, startAfter, limit }) {
      const response = await options.client.send(new ListObjectsV2Command({
        Bucket: options.bucket,
        Prefix: `${keyPrefix}${prefix}`,
        StartAfter: startAfter === undefined ? undefined : `${keyPrefix}${startAfter}`,
        MaxKeys: limit,
      }))
      return (response.Contents ?? []).flatMap(object => {
        if (object.Key === undefined || object.Size === undefined) return []
        return [{ key: object.Key.slice(keyPrefix.length), size: object.Size }]
      })
    },

    async get(key) {
      try {
        const response = await options.client.send(new GetObjectCommand({
          Bucket: options.bucket,
          Key: `${keyPrefix}${key}`,
        }))
        if (response.Body === undefined) return new Uint8Array()
        return await response.Body.transformToByteArray()
      } catch (error) {
        if (
          error instanceof NoSuchKey
          || (typeof error === 'object' && error !== null && '$metadata' in error
            && (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode === 404)
        ) {
          return null
        }
        throw error
      }
    },

    async put({ key, data, checksumSha256 }) {
      await options.client.send(new PutObjectCommand({
        Bucket: options.bucket,
        Key: `${keyPrefix}${key}`,
        Body: data,
        ChecksumSHA256: checksumSha256,
      }))
    },
  }
}

function normalizePrefix(value: string): string {
  if (value === '') return ''
  return value.endsWith('/') ? value : `${value}/`
}
