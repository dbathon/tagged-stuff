<script setup lang="ts">
import { defineProps } from "vue";

const props = defineProps<{
  label: string;
}>();
</script>

<template>
  <div>
    <label>
      {{ props.label }}:
      <slot />
    </label>
  </div>
</template>
