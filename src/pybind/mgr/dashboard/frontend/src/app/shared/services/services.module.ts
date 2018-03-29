import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { AuthGuardService } from './auth-guard.service';
import { AuthStorageService } from './auth-storage.service';
import { FormatterService } from './formatter.service';
import { SummaryService } from './summary.service';

@NgModule({
  imports: [CommonModule],
  declarations: [],
  providers: [
    AuthGuardService,
    AuthStorageService,
    FormatterService,
    SummaryService
  ]
})
export class ServicesModule {}
