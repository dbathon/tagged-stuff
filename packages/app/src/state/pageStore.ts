import { PostgrestPageStoreBackend, SimpleKeyValuePageStoreBackend } from "page-store-backends";
import { CompressingPageStoreBackend, InMemoryPageStoreBackend, PageStore, type PageStoreBackend } from "page-store";
import { shallowRef } from "vue";

const SETTINGS_KEY = "taggedStuff.pageStoreSettings";

const DEFAULT_PAGE_SIZE = 8192;

export type PageStoreBackendType = "InMemory" | "SimpleKeyValue" | "Postgrest";

export type PageStoreSettings = {
  backendType: PageStoreBackendType;
  backendUrl?: string;
  backendSecret?: string;
  backendStoreName?: string;
  useCompression: boolean;
  pageSize?: number;
  maxIndexPageSize?: number;
  // TODO: more settings
};

/**
 * The currently active/used pageStore (can be undefined).
 */
export const pageStore = shallowRef<PageStore>();

function constructBaseBackend(settings: PageStoreSettings): PageStoreBackend {
  switch (settings.backendType) {
    case "InMemory":
      return new InMemoryPageStoreBackend();
    case "SimpleKeyValue":
      return new SimpleKeyValuePageStoreBackend(settings.backendUrl || "-", settings.backendSecret || "-");
    case "Postgrest":
      return new PostgrestPageStoreBackend(
        settings.backendUrl || "-",
        settings.backendSecret || "-",
        settings.backendStoreName || "-",
      );
    default:
      throw new Error("unexpected backendType: " + settings.backendType);
  }
}

function updatePageStoreInstance(settings: PageStoreSettings): void {
  try {
    let backend = constructBaseBackend(settings);

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
    backendType: "InMemory",
    useCompression: true,
    pageSize: DEFAULT_PAGE_SIZE,
  };
}

export function savePageStoreSettings(settings: PageStoreSettings) {
  // do some cleanup
  if (settings.backendType === "InMemory") {
    settings.backendUrl = undefined;
    settings.backendSecret = undefined;
  }
  if (settings.backendType !== "Postgrest") {
    settings.backendStoreName = undefined;
  }

  window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
  updatePageStoreInstance(settings);
}

// set initial value
updatePageStoreInstance(getPageStoreSettings());
