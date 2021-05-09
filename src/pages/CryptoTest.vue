<script setup lang="ts">

async function test() {
  let start = new Date().getTime();

  const input = "secret text";
  const encoded = new TextEncoder().encode(input);
  console.log([...new Int8Array(encoded)]);


  const subtleCrypto = crypto.subtle;

  const key = await subtleCrypto.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt", "decrypt"]);

  console.log(key);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const params: AesCbcParams = { name: "AES-GCM", iv }
  const encrypted = await subtleCrypto.encrypt(params, key, encoded);

  const cnt = 100000
  if ("z".length === 0) {
    for (let i = 0; i < cnt; ++i) {
      await subtleCrypto.encrypt(params, key, encoded);
      // await subtleCrypto.digest("SHA-256", encoded);
    }
  }
  else {
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
</script>

<template>
  <button @click="test()">Crypto Test</button>
</template>

<style scoped>
</style>
