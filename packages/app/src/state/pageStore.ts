import { CompressingPageStoreBackend } from "@/data/page-store/CompressingPageStoreBackend";
import { InMemoryPageStoreBackend } from "@/data/page-store/InMemoryPageStoreBackend";
import { PageStore } from "@/data/page-store/PageStore";
import type { PageStoreBackend } from "@/data/page-store/PageStoreBackend";
import { shallowRef } from "vue";

const SETTINGS_KEY = "taggedStuff.pageStoreSettings";

const DEFAULT_PAGE_SIZE = 8192;

export type PageStoreSettings = {
  useCompression: boolean;
  pageSize?: number;
  maxIndexPageSize?: number;
  // TODO: more settings
};

/**
 * The currently active/used pageStore (can be undefined).
 */
export const pageStore = shallowRef<PageStore>();

function updatePageStoreInstance(settings: PageStoreSettings): void {
  try {
    let backend: PageStoreBackend = new InMemoryPageStoreBackend();
    if (settings.useCompression) {
      backend = new CompressingPageStoreBackend(backend);
    }

    const pageSize = settings.pageSize ?? DEFAULT_PAGE_SIZE;
    const maxIndexPageSize = settings.maxIndexPageSize ?? pageSize;
    pageStore.value = new PageStore(backend, pageSize, maxIndexPageSize);
  } catch (e) {
    pageStore.value = undefined;
    console.warn("failed to build page store", e);
  }
}

export function getPageStoreSettings(): PageStoreSettings {
  const settingsJson = window.localStorage.getItem(SETTINGS_KEY);
  if (settingsJson) {
    const result: PageStoreSettings = JSON.parse(settingsJson);
    if (result) {
      return result;
    }
  }
  // construct the default settings
  return {
    useCompression: true,
    pageSize: DEFAULT_PAGE_SIZE,
  };
}

export function savePageStoreSettings(settings: PageStoreSettings) {
  window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
  updatePageStoreInstance(settings);
}

// set initial value
updatePageStoreInstance(getPageStoreSettings());
