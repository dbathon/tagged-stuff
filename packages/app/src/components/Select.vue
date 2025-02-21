<script setup lang="ts" generic="T extends string">
import { defineProps } from "vue";
import LabelWrapper from "./LabelWrapper.vue";
import type { SelectOption } from "./types";

const props = defineProps<{
  label: string;
  options: SelectOption<T>[];
}>();

const model = defineModel<T>({
  set(newValue) {
    if (props.options.some((option) => option.value === newValue)) {
      return newValue;
    }
    return undefined;
  },
});
</script>

<template>
  <LabelWrapper :label="props.label">
    <select v-model="model">
      <option v-for="option in props.options" :value="option.value">
        {{ option.label }}
      </option>
    </select>
  </LabelWrapper>
</template>
