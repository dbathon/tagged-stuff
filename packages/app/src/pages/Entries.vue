<script setup lang="ts">
import { reactive, ref } from "vue";
import { computed } from "vue";
import { CachingDataStoreBackend } from "../shared/caching-data-store-backend";
import { DataStore } from "../shared/data-store";
import type { DataStoreBackend } from "../shared/data-store";
import type { Entry } from "../shared/entry/entry";
import { EntryService } from "../shared/entry/entry-service";
import type { DatabaseInformation } from "../shared/jds-client";
import { JdsDataStoreBackend } from "../shared/jds-data-store-backend";
import { getSettings } from "../shared/settings";
import { EncryptingDataStoreBackend } from "../shared/encrypting-data-store-backend";

const settings = getSettings();
if (settings === undefined || settings.jdsUrl === undefined) {
  throw new Error("jdsUrl not available");
}
const storeId = settings.storeId || "store";
const jdsDataStoreBackend = new JdsDataStoreBackend(settings.jdsUrl, storeId);
const cachingDataStoreBackend = new CachingDataStoreBackend(
  jdsDataStoreBackend,
  JSON.stringify([settings.jdsUrl, storeId]),
);
let dataStoreBackend: DataStoreBackend = cachingDataStoreBackend;
if (settings.secret !== undefined && settings.secret.length > 0) {
  dataStoreBackend = new EncryptingDataStoreBackend(dataStoreBackend, settings.secret);
}
const dataStore = new DataStore(dataStoreBackend);
const entryService = new EntryService(dataStore);

const databaseInformation = ref<DatabaseInformation>();

const entries = reactive<Entry[]>([]);
const activeEntry = ref<Entry>();

function refreshData() {
  jdsDataStoreBackend.jdsClient.getDatabaseInformation().then((info) => (databaseInformation.value = info));

  entryService.query().then((result) => {
    entries.length = 0;
    entries.push(...result);
  });
}
refreshData();

function newEntry() {
  editEntry({});
}

const formValues = reactive({
  title: "",
});

const formValid = computed(() => formValues.title !== undefined && formValues.title.length > 0);

function editEntry(entry: Entry) {
  activeEntry.value = entry;
  formValues.title = entry.title || "";
}

function saveEntry() {
  if (activeEntry.value && formValid.value) {
    activeEntry.value.title = formValues.title;

    if (activeEntry.value.id === undefined) {
      entries.push(activeEntry.value);
    }
    entryService.save(activeEntry.value);
    activeEntry.value = undefined;
  }
}

function deleteEntry(entry: Entry) {
  const index = entries.indexOf(entry);
  if (index >= 0) {
    entries.splice(index, 1);
    entryService.delete(entry);
  }
  if (activeEntry.value === entry) {
    activeEntry.value = undefined;
  }
}
</script>

<template>
  <h2>Entries</h2>
  <p>Database name: {{ databaseInformation?.name }}</p>

  <hr />

  <button @click="newEntry()">New entry</button>

  <div v-if="activeEntry">
    <form @submit.prevent="saveEntry()">
      <label>
        Title
        <input type="text" v-model="formValues.title" />
      </label>
      <!-- TODO: somehow handle "disabled if pristine"... -->
      <button type="submit" :disabled="!formValid">Save</button>
    </form>
  </div>

  <div>{{ entries.length }} entries</div>

  <div v-for="entry in entries" :key="entry.id">
    <h4>{{ entry.title }}</h4>
    <p>id: {{ entry.id }}</p>
    <p>version: {{ entry.version }}</p>

    <button @click="editEntry(entry)">Edit</button>
    <button @click="deleteEntry(entry)">Delete</button>
  </div>
</template>

<style scoped></style>
