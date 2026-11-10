/**
 * The cipher of `PersonalData` on `node:crypto`. WebCrypto, the default, is
 * asynchronous, which makes each personal field cost a trip through the event
 * loop. This one encrypts and decrypts synchronously, in the same format, so
 * data written with either reads with the other.
 *
 * ```ts
 * const Keys = PersonalData.layerSql().pipe(
 *   Layer.provide(PersonalData.layerMasterKeyConfig()),
 *   Layer.provide(PersonalDataNode.layerCipher)
 * )
 * ```
 *
 * It lives in a module of its own so that code for browsers never imports `node:crypto`.
 */
import { createCipheriv, createDecipheriv, createSecretKey, type KeyObject, randomFillSync } from "node:crypto"
import { Cause, Effect, Layer } from "effect"
import { Cipher, type CipherKey, ivLength, keyLength, tagLength } from "./PersonalData.ts"

const algorithm = "aes-256-gcm"

const asKeyObject = (key: CipherKey) => key as unknown as KeyObject

/** AES-256-GCM on `node:crypto`. */
export const cipher: Cipher = {
  asynchronous: false,
  importKey: (bytes) =>
    bytes.byteLength !== keyLength
      ? Effect.fail(new Cause.UnknownError(undefined, `The key has ${bytes.byteLength} bytes instead of ${keyLength}`))
      // A `KeyObject` keeps a copy of its own, so the caller can zero the bytes.
      : Effect.try(() => createSecretKey(bytes) as unknown as CipherKey),
  encrypt: (key, plaintext, additionalData) =>
    Effect.try(() => {
      const sealed = new Uint8Array(ivLength + plaintext.byteLength + tagLength)
      const iv = randomFillSync(sealed, 0, ivLength).subarray(0, ivLength)
      const encryption = createCipheriv(algorithm, asKeyObject(key), iv, { authTagLength: tagLength })
      encryption.setAAD(additionalData)
      let offset = ivLength
      for (const chunk of [encryption.update(plaintext), encryption.final()]) {
        sealed.set(chunk, offset)
        offset += chunk.byteLength
      }
      sealed.set(encryption.getAuthTag(), offset)
      return sealed
    }),
  decrypt: (key, sealed, additionalData) =>
    Effect.try(() => {
      if (sealed.byteLength < ivLength + tagLength) throw new Error("The data is too short")
      const decryption = createDecipheriv(algorithm, asKeyObject(key), sealed.subarray(0, ivLength), {
        authTagLength: tagLength
      })
      decryption.setAAD(additionalData)
      decryption.setAuthTag(sealed.subarray(sealed.byteLength - tagLength))
      const head = decryption.update(sealed.subarray(ivLength, sealed.byteLength - tagLength))
      // `final` checks the tag, and throws when the data was altered.
      const tail = decryption.final()
      const plaintext = new Uint8Array(head.byteLength + tail.byteLength)
      plaintext.set(head)
      plaintext.set(tail, head.byteLength)
      head.fill(0)
      return plaintext
    })
}

/** Provides {@link cipher} to the keys and master keys built with it. */
export const layerCipher: Layer.Layer<never> = Layer.succeed(Cipher, cipher)
