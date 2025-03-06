import { createRouter, createWebHistory, type NavigationGuardWithThis } from "vue-router";
import JsonStoreTest from "./pages/JsonStoreTest.vue";
import ConcurrencyTest from "./pages/ConcurrencyTest.vue";
import Settings from "./pages/Settings.vue";
import NotFound from "./pages/NotFound.vue";
import { isPageStoreAvailable } from "./state/pageStore";

const beforeEnter: NavigationGuardWithThis<undefined> = async (to, from, next) => {
  if (!(await isPageStoreAvailable())) {
    // redirect to settings, if no pageStore is available
    next({ name: "Settings" });
  } else {
    next();
  }
};

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: "/tagged-stuff/json-store-test",
      name: "JsonStoreTest",
      component: JsonStoreTest,
      beforeEnter,
    },
    {
      path: "/tagged-stuff/concurrency-test",
      name: "ConcurrencyTest",
      component: ConcurrencyTest,
      beforeEnter,
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
