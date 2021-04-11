import { createRouter, createWebHistory } from "vue-router";
import Entries from "./pages/Entries.vue";
import BTreeTest from "./pages/BTreeTest.vue";
import Settings from "./pages/Settings.vue";
import NotFound from "./pages/NotFound.vue";
import { getSettings } from "./shared/settings";

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
        }
        next();
      }
    },
    {
      path: "/tagged-stuff/b-tree-test",
      name: "BTreeTest",
      component: BTreeTest
    },
    {
      path: "/tagged-stuff/settings",
      name: "Settings",
      component: Settings
    },
    {
      path: "/tagged-stuff",
      redirect: "/tagged-stuff/entries"
    },
    {
      path: "/",
      redirect: "/tagged-stuff"
    },
    {
      path: "/:pathMatch(.*)*",
      component: NotFound
    },
  ],
});
