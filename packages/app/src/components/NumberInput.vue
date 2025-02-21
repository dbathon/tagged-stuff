<script setup lang="ts">
import { defineProps } from "vue";
import LabelWrapper from "./LabelWrapper.vue";

const props = defineProps<{
  label: string;
}>();

const model = defineModel<number>({
  set(newValue) {
    if (typeof newValue === "number" && !Number.isNaN(newValue)) {
      return newValue;
    }
    return undefined;
  },
});
</script>

<template>
  <LabelWrapper :label="props.label">
    <input type="number" v-model="model" />
  </LabelWrapper>
</template>
