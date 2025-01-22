<script setup lang="ts">
import { useRouter } from "vue-router";
import { getPageStoreSettings, savePageStoreSettings } from "@/state/pageStore";
import { ref } from "vue";

const router = useRouter();

const previousSettings = getPageStoreSettings();

const useCompression = ref(previousSettings.useCompression);
const pageSize = ref(previousSettings.pageSize);
const maxIndexPageSize = ref(previousSettings.maxIndexPageSize);

function save(navigate: boolean) {
  savePageStoreSettings({
    useCompression: useCompression.value,
    pageSize: pageSize.value || undefined,
    maxIndexPageSize: maxIndexPageSize.value || undefined,
  });

  if (navigate) {
    router.push("/");
  }
}
</script>

<template>
  <h2>Settings</h2>
  <form @submit.prevent="save(true)">
    <div>
      <label>
        Use compression:
        <input v-model="useCompression" type="checkbox" />
      </label>
    </div>
    <div>
      <label>
        Page size:
        <input v-model="pageSize" type="number" />
      </label>
    </div>
    <div>
      <label>
        Maximum index page size:
        <input v-model="maxIndexPageSize" type="number" />
      </label>
    </div>

    <div>
      <button type="submit">Save</button>
    </div>
  </form>
</template>

<style scoped></style>
