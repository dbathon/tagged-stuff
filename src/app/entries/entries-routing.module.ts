import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { EntriesComponent } from './entries.component';
import { SettingsAvailableGuard } from '../shared/settings-available.guard';


const routes: Routes = [
  {
    path: 'tagged-stuff/entries', component: EntriesComponent, canActivate: [SettingsAvailableGuard]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class EntriesRoutingModule { }
