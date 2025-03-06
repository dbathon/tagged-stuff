<script setup lang="ts">
import { onScopeDispose, ref, watchEffect } from "vue";
import { pageStore, pageStoreTransaction } from "@/state/pageStore";
import { useJsonQuery } from "./json-store/useJsonQuery";
import { deleteJson, queryJson, saveJson } from "json-store";
import NumberInput from "@/components/NumberInput.vue";
import CheckboxInput from "@/components/CheckboxInput.vue";

const TABLE_NAME = "_concurrent_";

type Entry = {
  id?: number;
  key: string;
  count: number;
};

const entries = useJsonQuery<Entry>(pageStore, { table: TABLE_NAME });

const key = "K" + Math.random();

const intervalMillis = ref(1000);
const active = ref(false);
const inTransaction = ref(false);

let handle: number | undefined = undefined;

function cleanup() {
  if (handle !== undefined) {
    clearTimeout(handle);
    handle = undefined;
  }
}

onScopeDispose(cleanup);

function setupTimeout() {
  cleanup();
  if (active.value && !inTransaction.value && intervalMillis.value > 0) {
    handle = setTimeout(() => {
      increment();
    }, intervalMillis.value);
  }
}

watchEffect(setupTimeout);

async function increment() {
  inTransaction.value = true;
  await pageStoreTransaction((pageAccess) => {
    const entry = queryJson<Entry>(pageAccess.get, { table: TABLE_NAME, filter: ["key", "=", key] })[0] ?? {
      key,
      count: 0,
    };
    ++entry.count;
    saveJson(pageAccess, TABLE_NAME, entry);
  });
  inTransaction.value = false;
  setupTimeout();
}

async function deleteEntry(id: number) {
  await pageStoreTransaction((pageAccess) => {
    deleteJson(pageAccess, TABLE_NAME, id);
  });
}
</script>

<template>
  <h1>Settings</h1>
  <div>Key: {{ key }}</div>
  <NumberInput label="Interval" v-model="intervalMillis" />
  <CheckboxInput label="Active" v-model="active" />
  <div>In transaction: {{ inTransaction }}</div>

  <h1>Entries</h1>
  <div v-for="entry in entries || []">
    <div>
      ID: {{ entry.id }}, Key: {{ entry.key }}, Count: {{ entry.count }}
      <button @click="deleteEntry(entry.id!)">Delete</button>
    </div>
  </div>
</template>
