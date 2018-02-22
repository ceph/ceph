import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ConfigurationService } from './configuration.service';
import { FormatterService } from './formatter.service';
import { SummaryService } from './summary.service';
import { TcmuIscsiService } from './tcmu-iscsi.service';

@NgModule({
  imports: [
    CommonModule
  ],
  declarations: [],
  providers: [FormatterService, SummaryService, TcmuIscsiService, ConfigurationService]
})
export class ServicesModule { }
