import { Component, OnInit } from '@angular/core';
import { Settings, getSettings, saveSettings } from '../shared/settings';
import { Router } from '@angular/router';

@Component({
  selector: 'app-settings',
  templateUrl: './settings.component.html',
  styles: [
  ],
})
export class SettingsComponent implements OnInit {

  settings: Settings;

  constructor(private router: Router) {
    this.settings = getSettings() || new Settings();
  }

  ngOnInit(): void {
  }

  saveSettings(navigate: boolean) {
    saveSettings(this.settings);

    if (navigate) {
      this.router.navigate(['']);
    }
  }

}
