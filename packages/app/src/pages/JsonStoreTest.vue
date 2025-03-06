<script setup lang="ts">
import { ref } from "vue";
import JsonStoreTestTable from "./json-store/JsonStoreTestTable.vue";
import { pageStore, pageStoreTransaction } from "@/state/pageStore";
import { useJsonQuery } from "./json-store/useJsonQuery";
import { countJson, saveJson } from "json-store";

const TABLES_TABLE_NAME = "_tables_";

const tables = useJsonQuery<{ name: string }>(pageStore, { table: TABLES_TABLE_NAME });
const newTableName = ref("");

async function addTable() {
  const tableName = newTableName.value;
  if (tableName) {
    await pageStoreTransaction((pageAccess) => {
      const existingCount = countJson(pageAccess.get, { table: TABLES_TABLE_NAME, filter: ["name", "=", tableName] });
      if (!existingCount) {
        saveJson(pageAccess, TABLES_TABLE_NAME, { name: tableName });
      } else {
        console.log("table already exists", tableName);
      }
    });

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
  <div>
    <button @click="pageStore?.refresh()">Refresh</button>
  </div>
  <h1>Tables</h1>
  <div v-for="table in tables || []">
    <JsonStoreTestTable :tableName="table.name" />
    <hr />
  </div>
</template>
