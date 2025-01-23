<script setup lang="ts">
const title = "tagged-stuff";

const routeLinks = [
  ["JsonStoreTest", "JsonStoreTest"],
  ["Settings", "Settings"],
];
</script>

<template>
  <h1>Welcome to {{ title }}!</h1>

  <nav class="navList">
    <router-link v-for="link in routeLinks" :to="{ name: link[0] }" active-class="active">{{ link[1] }}</router-link>
  </nav>

  <router-view />
</template>

<style lang="scss" scoped>
.navList > a {
  margin: 2px;
  padding: 3px;
  border-radius: 3px;

  &.active {
    background-color: #d8d8d8;
  }
}
</style>
