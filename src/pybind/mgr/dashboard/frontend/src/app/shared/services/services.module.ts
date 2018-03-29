import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { FormatterService } from './formatter.service';
import { SummaryService } from './summary.service';

@NgModule({
  imports: [CommonModule],
  declarations: [],
  providers: [
    FormatterService,
    SummaryService
  ]
})
export class ServicesModule { }
