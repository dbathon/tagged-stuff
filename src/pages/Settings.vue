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
  <form @submit.prevent="save(true)">
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
      <button type="submit">Save</button>
    </div>
  </form>
</template>

<style scoped>
</style>
