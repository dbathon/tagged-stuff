<script setup lang="ts">
import { onScopeDispose, reactive, ref } from "vue";
import { InMemoryPageStoreBackend } from "../data/page-store/InMemoryPageStoreBackend";
import { PageStore } from "../data/page-store/PageStore";
import JsonStoreTestTable from "./json-store/JsonStoreTestTable.vue";

const backendPages = ref(0);
const backend = new InMemoryPageStoreBackend();
const pageStore = new PageStore(backend, 4096, 4096 * 4);

const interval = setInterval(() => {
  backendPages.value = backend.pages.size;
}, 100);
onScopeDispose(() => clearInterval(interval));

const tables: string[] = reactive([]);
const newTableName = ref("");

function addTable() {
  if (newTableName.value) {
    tables.push(newTableName.value);
    newTableName.value = "";
  }
}
</script>

<template>
  <h1>Add table</h1>
  <form @submit.prevent="addTable()">
    <label>
      Table name
      <input type="text" v-model="newTableName" />
    </label>
    <button type="submit">Add</button>
  </form>
  <h1>Backend pages: {{ backendPages }}</h1>
  <h1>Tables</h1>
  <div v-for="table in tables">
    <JsonStoreTestTable :tableName="table" :pageStore="pageStore" />
    <hr />
  </div>
</template>
