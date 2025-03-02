import { CompressingPageStoreBackend, PageStore, type PageStoreBackend } from "page-store";
import { Semaphore } from "shared-util";
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

export const pageStoreSetupSemaphore = new Semaphore(1);

async function constructBaseBackend(settings: PageStoreSettings): Promise<PageStoreBackend> {
  switch (settings.backendType) {
    case "InMemory":
      const { InMemoryPageStoreBackend } = await import("page-store");
      return new InMemoryPageStoreBackend();
    case "SimpleKeyValue":
      const { SimpleKeyValuePageStoreBackend } = await import(
        "page-store-backends/dist/SimpleKeyValuePageStoreBackend"
      );
      return new SimpleKeyValuePageStoreBackend(settings.backendUrl || "-", settings.backendSecret || "-");
    case "Postgrest":
      const { PostgrestPageStoreBackend } = await import("page-store-backends/dist/PostgrestPageStoreBackend");
      return new PostgrestPageStoreBackend(
        settings.backendUrl || "-",
        settings.backendSecret || "-",
        settings.backendStoreName || "-",
      );
    default:
      throw new Error("unexpected backendType: " + settings.backendType);
  }
}

function updatePageStoreInstance(settings: PageStoreSettings): Promise<void> {
  return pageStoreSetupSemaphore.run(async () => {
    try {
      let backend = await constructBaseBackend(settings);

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
  });
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

export async function savePageStoreSettings(settings: PageStoreSettings) {
  // do some cleanup
  if (settings.backendType === "InMemory") {
    settings.backendUrl = undefined;
    settings.backendSecret = undefined;
  }
  if (settings.backendType !== "Postgrest") {
    settings.backendStoreName = undefined;
  }

  window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
  await updatePageStoreInstance(settings);
}

export async function isPageStoreAvailable(): Promise<boolean> {
  return pageStoreSetupSemaphore.run(async () => pageStore.value !== undefined);
}

// set initial value
void updatePageStoreInstance(getPageStoreSettings());
