import { Component, OnInit } from '@angular/core';
import { SettingsService, Settings } from '../shared/settings.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-settings',
  templateUrl: './settings.component.html',
  styles: [
  ],
})
export class SettingsComponent implements OnInit {

  settings: Settings;

  constructor(private settingsService: SettingsService, private router: Router) {
    this.settings = settingsService.settings || new Settings();
  }

  ngOnInit(): void {
  }

  saveSettings(navigate: boolean) {
    this.settingsService.saveSetting(this.settings);

    if (navigate) {
      this.router.navigate(['']);
    }
  }

}
