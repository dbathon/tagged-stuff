import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SettingsRoutingModule } from './settings-routing.module';
import { SettingsComponent } from './settings.component';
import { FormsModule } from '@angular/forms';


@NgModule({
  declarations: [SettingsComponent],
  imports: [
    CommonModule,
    FormsModule,
    SettingsRoutingModule
  ]
})
export class SettingsModule { }
