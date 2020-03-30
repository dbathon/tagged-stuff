import { Injectable } from '@angular/core';

export class Settings {
  jdsUrl?: string;
}

const SETTINGS_KEY = "taggedStuff.settings";

@Injectable({
  providedIn: 'root'
})
export class SettingsService {

  /**
   * The currently active settings.
   */
  private activeSettings?: Settings;

  constructor() {
    const settingsJson = window.localStorage.getItem(SETTINGS_KEY);
    if (settingsJson) {
      this.activeSettings = JSON.parse(settingsJson);
    }
  }

  get settings(): Settings | undefined {
    return this.activeSettings && { ...this.activeSettings };
  }

  get settingsAvailable(): boolean {
    return !!this.activeSettings;
  }

  saveSetting(settings: Settings) {
    this.activeSettings = { ...settings };
    window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(this.activeSettings));
  }

}
