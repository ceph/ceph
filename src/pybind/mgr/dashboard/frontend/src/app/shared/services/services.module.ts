import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { AuthGuardService } from './auth-guard.service';
import { AuthStorageService } from './auth-storage.service';
import { FormatterService } from './formatter.service';
import { ModuleStatusGuardService } from './module-status-guard.service';
import { NotificationService } from './notification.service';
import { SummaryService } from './summary.service';
import { TaskManagerMessageService } from './task-manager-message.service';
import { TaskManagerService } from './task-manager.service';

@NgModule({
  imports: [CommonModule],
  declarations: [],
  providers: [
    AuthGuardService,
    AuthStorageService,
    FormatterService,
    SummaryService,
    ModuleStatusGuardService,
    NotificationService,
    TaskManagerService,
    TaskManagerMessageService
  ]
})
export class ServicesModule {}
