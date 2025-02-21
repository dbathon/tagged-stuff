<script setup lang="ts">
import { useRouter } from "vue-router";
import { getPageStoreSettings, savePageStoreSettings } from "@/state/pageStore";
import { reactive } from "vue";
import CheckboxInput from "@/components/CheckboxInput.vue";
import NumberInput from "@/components/NumberInput.vue";

const router = useRouter();

const settings = reactive(structuredClone(getPageStoreSettings()));

function save(navigate: boolean) {
  savePageStoreSettings(settings);

  if (navigate) {
    router.push("/");
  }
}
</script>

<template>
  <h2>Settings</h2>
  <form @submit.prevent="save(true)">
    <CheckboxInput label="Use compression" v-model="settings.useCompression" />
    <NumberInput label="Page size" v-model="settings.pageSize" />
    <NumberInput label="Maximum index page size" v-model="settings.maxIndexPageSize" />

    <div>
      <button type="submit">Save</button>
    </div>
  </form>
</template>

<style scoped></style>
