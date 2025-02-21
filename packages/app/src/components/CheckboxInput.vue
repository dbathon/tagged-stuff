<script setup lang="ts">
import { defineProps } from "vue";
import LabelWrapper from "./LabelWrapper.vue";

const props = defineProps<{
  label: string;
}>();

const model = defineModel<boolean>({
  set(newValue) {
    return !!newValue;
  },
});
</script>

<template>
  <LabelWrapper :label="props.label">
    <input type="checkbox" v-model="model" />
  </LabelWrapper>
</template>
