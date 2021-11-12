import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import {
  NgbAlertModule,
  NgbDatepickerModule,
  NgbDropdownModule,
  NgbPopoverModule,
  NgbProgressbarModule,
  NgbTimepickerModule,
  NgbTooltipModule
} from '@ng-bootstrap/ng-bootstrap';
import { ClickOutsideModule } from 'ng-click-outside';
import { ChartsModule } from 'ng2-charts';
import { SimplebarAngularModule } from 'simplebar-angular';

import { MotdComponent } from '~/app/shared/components/motd/motd.component';
import { DirectivesModule } from '../directives/directives.module';
import { PipesModule } from '../pipes/pipes.module';
import { AlertPanelComponent } from './alert-panel/alert-panel.component';
import { BackButtonComponent } from './back-button/back-button.component';
import { ConfigOptionComponent } from './config-option/config-option.component';
import { ConfirmationModalComponent } from './confirmation-modal/confirmation-modal.component';
import { Copy2ClipboardButtonComponent } from './copy2clipboard-button/copy2clipboard-button.component';
import { CriticalConfirmationModalComponent } from './critical-confirmation-modal/critical-confirmation-modal.component';
import { DateTimePickerComponent } from './date-time-picker/date-time-picker.component';
import { DocComponent } from './doc/doc.component';
import { DownloadButtonComponent } from './download-button/download-button.component';
import { FormButtonPanelComponent } from './form-button-panel/form-button-panel.component';
import { FormModalComponent } from './form-modal/form-modal.component';
import { GrafanaComponent } from './grafana/grafana.component';
import { HelperComponent } from './helper/helper.component';
import { LanguageSelectorComponent } from './language-selector/language-selector.component';
import { LoadingPanelComponent } from './loading-panel/loading-panel.component';
import { ModalComponent } from './modal/modal.component';
import { NotificationsSidebarComponent } from './notifications-sidebar/notifications-sidebar.component';
import { OrchestratorDocPanelComponent } from './orchestrator-doc-panel/orchestrator-doc-panel.component';
import { PwdExpirationNotificationComponent } from './pwd-expiration-notification/pwd-expiration-notification.component';
import { RefreshSelectorComponent } from './refresh-selector/refresh-selector.component';
import { SelectBadgesComponent } from './select-badges/select-badges.component';
import { SelectComponent } from './select/select.component';
import { SparklineComponent } from './sparkline/sparkline.component';
import { SubmitButtonComponent } from './submit-button/submit-button.component';
import { TelemetryNotificationComponent } from './telemetry-notification/telemetry-notification.component';
import { UsageBarComponent } from './usage-bar/usage-bar.component';
import { WizardComponent } from './wizard/wizard.component';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    NgbAlertModule,
    NgbPopoverModule,
    NgbProgressbarModule,
    NgbTooltipModule,
    ChartsModule,
    ReactiveFormsModule,
    PipesModule,
    DirectivesModule,
    NgbDropdownModule,
    ClickOutsideModule,
    SimplebarAngularModule,
    RouterModule,
    NgbDatepickerModule,
    NgbTimepickerModule
  ],
  declarations: [
    SparklineComponent,
    HelperComponent,
    SelectBadgesComponent,
    SubmitButtonComponent,
    UsageBarComponent,
    LoadingPanelComponent,
    ModalComponent,
    NotificationsSidebarComponent,
    CriticalConfirmationModalComponent,
    ConfirmationModalComponent,
    LanguageSelectorComponent,
    GrafanaComponent,
    SelectComponent,
    BackButtonComponent,
    RefreshSelectorComponent,
    ConfigOptionComponent,
    AlertPanelComponent,
    FormModalComponent,
    PwdExpirationNotificationComponent,
    TelemetryNotificationComponent,
    OrchestratorDocPanelComponent,
    DateTimePickerComponent,
    DocComponent,
    Copy2ClipboardButtonComponent,
    DownloadButtonComponent,
    FormButtonPanelComponent,
    MotdComponent,
    WizardComponent
  ],
  providers: [],
  exports: [
    SparklineComponent,
    HelperComponent,
    SelectBadgesComponent,
    SubmitButtonComponent,
    BackButtonComponent,
    LoadingPanelComponent,
    UsageBarComponent,
    ModalComponent,
    NotificationsSidebarComponent,
    LanguageSelectorComponent,
    GrafanaComponent,
    SelectComponent,
    RefreshSelectorComponent,
    ConfigOptionComponent,
    AlertPanelComponent,
    PwdExpirationNotificationComponent,
    TelemetryNotificationComponent,
    OrchestratorDocPanelComponent,
    DateTimePickerComponent,
    DocComponent,
    Copy2ClipboardButtonComponent,
    DownloadButtonComponent,
    FormButtonPanelComponent,
    MotdComponent,
    WizardComponent
  ]
})
export class ComponentsModule {}
