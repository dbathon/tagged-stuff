import { Injectable } from '@angular/core';
import { JdsClientService } from './jds-client.service';
import { Subject, BehaviorSubject } from 'rxjs';

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

  readonly settings$: BehaviorSubject<Settings | undefined>;

  constructor() {
    const settingsJson = window.localStorage.getItem(SETTINGS_KEY);
    if (settingsJson) {
      this.activeSettings = JSON.parse(settingsJson);
    }
    this.settings$ = new BehaviorSubject(this.settings);
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

    this.settings$.next(this.settings);
  }

}
