import { createApp } from "vue";
import App from "./App.vue";
import { router } from "./router";
import { enableMessagesForAssert } from "shared-util";

if (import.meta.env.DEV) {
  enableMessagesForAssert();
}

const app = createApp(App);
app.use(router);

app.mount("#app");
