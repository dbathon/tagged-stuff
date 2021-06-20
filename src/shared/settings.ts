
export class Settings {
  jdsUrl?: string;
  storeId?: string;
  secret?: string;
}

const SETTINGS_KEY = "taggedStuff.settings";

let activeSettings: Settings | undefined;

export function getSettings(): Settings | undefined {
  if (activeSettings === undefined) {
    // try to lazy init
    const settingsJson = window.localStorage.getItem(SETTINGS_KEY);
    if (settingsJson) {
      activeSettings = JSON.parse(settingsJson);
    }
  }
  return activeSettings && { ...activeSettings };
}

export function saveSettings(settings: Settings) {
  activeSettings = { ...settings };
  window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(activeSettings));
}
