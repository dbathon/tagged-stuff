import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { EntriesRoutingModule } from './entries-routing.module';
import { EntriesComponent } from './entries.component';
import { FormsModule, ReactiveFormsModule } from "@angular/forms";


@NgModule({
  declarations: [EntriesComponent],
  imports: [
    CommonModule,
    ReactiveFormsModule,
    FormsModule,
    EntriesRoutingModule
  ]
})
export class EntriesModule { }
