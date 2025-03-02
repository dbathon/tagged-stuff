<script setup lang="ts">
import { useRouter } from "vue-router";
import { getPageStoreSettings, savePageStoreSettings, type PageStoreBackendType } from "@/state/pageStore";
import { reactive } from "vue";
import CheckboxInput from "@/components/CheckboxInput.vue";
import NumberInput from "@/components/NumberInput.vue";
import type { SelectOption } from "@/components/types";
import Select from "@/components/Select.vue";
import TextInput from "@/components/TextInput.vue";

const router = useRouter();

const settings = reactive(structuredClone(getPageStoreSettings()));

const backendTypes: SelectOption<PageStoreBackendType>[] = [
  { label: "In memory", value: "InMemory" },
  { label: "Simple key value serverless function", value: "SimpleKeyValue" },
  { label: "Postgrest", value: "Postgrest" },
];

async function save(navigate: boolean) {
  await savePageStoreSettings(settings);

  if (navigate) {
    router.push("/");
  }
}
</script>

<template>
  <h2>Settings</h2>
  <form @submit.prevent="save(true)">
    <Select label="Backend type" :options="backendTypes" v-model="settings.backendType" />
    <TextInput label="Backend URL" v-model="settings.backendUrl" v-if="settings.backendType !== 'InMemory'" />
    <TextInput label="Backend secret" v-model="settings.backendSecret" v-if="settings.backendType !== 'InMemory'" />
    <TextInput
      label="Backend store name"
      v-model="settings.backendStoreName"
      v-if="settings.backendType === 'Postgrest'"
    />
    <CheckboxInput label="Use compression" v-model="settings.useCompression" />
    <NumberInput label="Page size" v-model="settings.pageSize" />
    <NumberInput label="Maximum index page size" v-model="settings.maxIndexPageSize" />

    <div>
      <button type="submit">Save</button>
    </div>
  </form>
</template>

<style scoped></style>
