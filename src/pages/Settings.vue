<script setup lang="ts">
import { useRouter } from "vue-router";
import { getSettings, saveSettings, Settings } from "../shared/settings";

const router = useRouter();
const settings = getSettings() || new Settings();

function save(navigate: boolean) {
  saveSettings(settings);

  if (navigate) {
    router.push('/');
  }
}
</script>

<template>
  <h2>Settings</h2>

  <div>
    <label>
      jds URL:
      <input v-model="settings.jdsUrl" placeholder="https://..." />
    </label>
  </div>
  <div>
    <label>
      Store Id:
      <input v-model="settings.storeId" />
    </label>
  </div>

  <div>
    <button @click="save(true)">Save</button>
  </div>
</template>

<style scoped>
</style>
