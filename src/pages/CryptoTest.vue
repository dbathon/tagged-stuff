<script setup lang="ts">
import { encodeBytes, decodeBytes } from "../shared/encode-bytes";

async function test() {
  let start = new Date().getTime();

  const input = "secret text";
  const encoded = new TextEncoder().encode(input);
  console.log([...new Int8Array(encoded)]);

  const subtleCrypto = crypto.subtle;

  const key = await subtleCrypto.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt", "decrypt"]);

  console.log(key);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const params: AesCbcParams = { name: "AES-GCM", iv };
  const encrypted = await subtleCrypto.encrypt(params, key, encoded);

  const cnt = 100000;
  if ("z".length === 0) {
    for (let i = 0; i < cnt; ++i) {
      await subtleCrypto.encrypt(params, key, encoded);
      // await subtleCrypto.digest("SHA-256", encoded);
    }
  } else {
    const promises: Promise<ArrayBuffer>[] = [];
    for (let i = 0; i < cnt; ++i) {
      promises.push(subtleCrypto.encrypt(params, key, encoded));
    }
    console.log("before await", new Date().getTime() - start);
    await Promise.all(promises);
  }

  const encryptedChanged = new Int8Array(encrypted);
  //encryptedChanged[0] -= 1;

  console.log([...new Int8Array(encrypted)]);

  const decrypted = await subtleCrypto.decrypt(params, key, encrypted);

  const decoded = new TextDecoder().decode(decrypted);

  console.log([input, decoded]);

  console.log("done", new Date().getTime() - start);
}

function checkEqual(array1: Uint8Array, array2: Uint8Array) {
  const length = array1.length;
  if (length !== array2.length) {
    throw Error("different length");
  }
  for (let i = 0; i < length; ++i) {
    if (array1[i] !== array2[i]) {
      throw Error("different data");
    }
  }
}

function testEncDecWithLength(length: number) {
  const buffer = new Uint8Array(length);
  console.log(encodeBytes(buffer));
  buffer[0] = 255;
  console.log(encodeBytes(buffer));
  buffer[length - 1] = 255;
  console.log(encodeBytes(buffer));
  for (let i = 0; i < length; ++i) {
    buffer[i] = 255;
  }
  console.log(encodeBytes(buffer));

  for (let i = 0; i < 10; ++i) {
    crypto.getRandomValues(buffer);
    console.log(buffer);
    const encoded = encodeBytes(buffer);
    console.log(encoded);
    checkEqual(buffer, decodeBytes(encoded));
  }
}

function testEncDec() {
  testEncDecWithLength(0);
  testEncDecWithLength(1);
  testEncDecWithLength(3);
  testEncDecWithLength(16);
}

async function test2() {
  try {
    const subtleCrypto = crypto.subtle;
    const buffer = new Uint8Array(16);

    crypto.getRandomValues(buffer);

    const orgKey = encodeBytes(buffer);
    const orgKeyBytes = decodeBytes(orgKey);
    console.log("orgKey", orgKey, orgKeyBytes);

    const keyEncryptKey = await subtleCrypto.importKey("raw", orgKeyBytes, "AES-CBC", false, ["encrypt"]);

    const encKeyBytes = new Uint8Array(
      await subtleCrypto.encrypt({ name: "AES-CBC", iv: new Uint8Array(16) }, keyEncryptKey, new Uint8Array(1))
    );
    const encKey = encodeBytes(encKeyBytes);

    console.log("encKey", encKey, encKeyBytes);

    const dataKey = await subtleCrypto.importKey("raw", orgKeyBytes, "AES-GCM", false, ["encrypt", "decrypt"]);
    const aesGcmParams: AesGcmParams = {
      name: "AES-GCM",
      iv: new Uint8Array(16),
      tagLength: 128,
    };

    const dataBytes = [
      new Uint8Array(1),
      new Uint8Array(5),
      new Uint8Array(20),
      new TextEncoder().encode("test"),
      new TextEncoder().encode("hello world"),
    ];
    for (const orgDataBytes of dataBytes) {
      const encDataBytes = new Uint8Array(await subtleCrypto.encrypt(aesGcmParams, dataKey, orgDataBytes));

      console.log("orgDataBytes", orgDataBytes);
      console.log("encDataBytes", encDataBytes);

      const decDataBytes = new Uint8Array(await subtleCrypto.decrypt(aesGcmParams, dataKey, encDataBytes));

      console.log("decDataBytes", decDataBytes);
      checkEqual(orgDataBytes, decDataBytes);
    }
  } catch (e) {
    console.error("something failed", e);
  }
}

function toHexString(byteArray: Uint8Array) {
  return Array.from(byteArray, (byte) => ("0" + byte.toString(16)).slice(-2)).join("");
}

async function testPbkdf2() {
  try {
    const subtleCrypto = crypto.subtle;

    const secretBytes = new TextEncoder().encode("secret");
    for (const iterations of [10000, 50000, 100000, 200000, 500000, 1000000, 5000000]) {
      const start = new Date().getTime();

      // zero salt for testing
      const salt = new Uint8Array(16);
      const rawKey = await subtleCrypto.importKey("raw", secretBytes, "PBKDF2", false, ["deriveKey"]);

      const key = await subtleCrypto.deriveKey(
        {
          name: "PBKDF2",
          salt: salt,
          iterations: iterations,
          hash: "SHA-256",
        },
        rawKey,
        {
          name: "AES-GCM",
          length: 128,
        },
        true,
        ["encrypt", "decrypt"]
      );

      const exportedKey = await subtleCrypto.exportKey("raw", key);

      console.log(iterations, new Date().getTime() - start, toHexString(new Uint8Array(exportedKey)));
    }
  } catch (e) {
    console.error("something failed", e);
  }
}
</script>

<template>
  <button @click="test()">Crypto Test</button>
  <button @click="testEncDec()">Encode/Decode Test</button>
  <button @click="test2()">Crypto Test 2</button>
  <button @click="testPbkdf2()">PBKDF2 Test</button>
</template>

<style scoped></style>
