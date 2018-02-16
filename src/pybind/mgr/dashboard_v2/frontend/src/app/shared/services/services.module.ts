import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { FormatterService } from './formatter.service';
import { TcmuIscsiService } from './tcmu-iscsi.service';
import { TopLevelService } from './top-level.service';

@NgModule({
  imports: [
    CommonModule
  ],
  declarations: [],
  providers: [FormatterService, TopLevelService, TcmuIscsiService]
})
export class ServicesModule { }
