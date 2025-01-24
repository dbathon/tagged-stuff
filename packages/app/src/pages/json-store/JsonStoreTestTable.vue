<script setup lang="ts">
import { ref } from "vue";
import { deleteJson, saveJson } from "json-store";
import { useJsonQuery } from "./useJsonQuery";
import { pageStore } from "@/state/pageStore";

interface TestEntry {
  id?: number;
  title?: string;
}

const props = defineProps<{ tableName: string }>();

const entries = useJsonQuery<TestEntry>(pageStore, () => ({ table: props.tableName }));

const activeEntry = ref<TestEntry>();

function editEntry(entry: TestEntry) {
  activeEntry.value = JSON.parse(JSON.stringify(entry));
}

function newEntry() {
  editEntry({});
}

async function saveEntry() {
  const entry = activeEntry.value;
  if (entry && pageStore.value) {
    await pageStore.value.runTransaction((pageAccess) => {
      saveJson(pageAccess, props.tableName, entry);
    });
    activeEntry.value = undefined;
  }
}

async function generateEntries() {
  const now = Date.now();
  for (let i = 0; i < 100; i++) {
    editEntry({ title: now + " - " + i });
    await saveEntry();
  }
}

async function generateEntriesFast() {
  const now = Date.now();
  await pageStore.value?.runTransaction((pageAccess) => {
    for (let i = 0; i < 100; i++) {
      saveJson(pageAccess, props.tableName, { title: now + " - " + i });
    }
  });
}

async function deleteEntry(entry: TestEntry) {
  await pageStore.value?.runTransaction((pageAccess) => {
    deleteJson(pageAccess, props.tableName, entry.id!);
  });
}
</script>

<template>
  <h2>{{ props.tableName }}</h2>

  <button @click="generateEntries()">Generate entries</button>
  <button @click="generateEntriesFast()">Generate entries fast</button>
  <button @click="newEntry()">New entry</button>

  <div v-if="activeEntry">
    <form @submit.prevent="saveEntry()">
      <label>
        Title
        <input type="text" v-model="activeEntry.title" />
      </label>
      <button type="submit">Save</button>
    </form>
  </div>

  <div v-if="!entries">Loading...</div>
  <div v-if="entries">
    <div>{{ entries.length }} entries</div>
    <div v-for="entry in entries" :key="entry.id">
      <p>
        {{ entry.title }} (id: {{ entry.id }})

        <button @click="editEntry(entry)">Edit</button>
        <button @click="deleteEntry(entry)">Delete</button>
      </p>
    </div>
  </div>
</template>
