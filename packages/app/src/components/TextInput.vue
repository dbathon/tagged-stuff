<script setup lang="ts">
import { defineProps } from "vue";
import LabelWrapper from "./LabelWrapper.vue";

const props = defineProps<{
  label: string;
}>();

const model = defineModel<string>({
  set(newValue) {
    if (newValue && typeof newValue === "string") {
      return newValue;
    }
    return undefined;
  },
});
</script>

<template>
  <LabelWrapper :label="props.label">
    <input type="text" v-model="model" />
  </LabelWrapper>
</template>
