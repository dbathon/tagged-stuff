import { createRouter, createWebHistory } from "vue-router";
import Entries from "./pages/Entries.vue";
import JsonStoreTest from "./pages/JsonStoreTest.vue";
import BTreeTest from "./pages/BTreeTest.vue";
import CryptoTest from "./pages/CryptoTest.vue";
import Settings from "./pages/Settings.vue";
import NotFound from "./pages/NotFound.vue";
import { getSettings } from "./shared/settings";
import { pageStore } from "./state/pageStore";

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: "/tagged-stuff/entries",
      name: "Entries",
      component: Entries,
      beforeEnter: (to, from, next) => {
        if (getSettings() === undefined) {
          // redirect to settings, if the settings are not configured yet
          next({ name: "Settings" });
        } else {
          next();
        }
      },
    },
    {
      path: "/tagged-stuff/json-store-test",
      name: "JsonStoreTest",
      component: JsonStoreTest,
      beforeEnter: (to, from, next) => {
        if (pageStore.value === undefined) {
          // redirect to settings, if no pageStore is available
          next({ name: "Settings" });
        } else {
          next();
        }
      },
    },
    {
      path: "/tagged-stuff/b-tree-test",
      name: "BTreeTest",
      component: BTreeTest,
    },
    {
      path: "/tagged-stuff/crypto-test",
      name: "CryptoTest",
      component: CryptoTest,
    },
    {
      path: "/tagged-stuff/settings",
      name: "Settings",
      component: Settings,
    },
    {
      path: "/tagged-stuff",
      redirect: "/tagged-stuff/json-store-test",
    },
    {
      path: "/",
      redirect: "/tagged-stuff",
    },
    {
      path: "/:pathMatch(.*)*",
      component: NotFound,
    },
  ],
});
