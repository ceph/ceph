import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';
import { ClickOutsideModule } from 'ng-click-outside';
import { ChartsModule } from 'ng2-charts';
import { AlertModule } from 'ngx-bootstrap/alert';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ModalModule } from 'ngx-bootstrap/modal';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { TooltipModule } from 'ngx-bootstrap/tooltip';
import { SimplebarAngularModule } from 'simplebar-angular';

import { DirectivesModule } from '../directives/directives.module';
import { PipesModule } from '../pipes/pipes.module';
import { AlertPanelComponent } from './alert-panel/alert-panel.component';
import { BackButtonComponent } from './back-button/back-button.component';
import { ConfigOptionComponent } from './config-option/config-option.component';
import { ConfirmationModalComponent } from './confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from './critical-confirmation-modal/critical-confirmation-modal.component';
import { FormModalComponent } from './form-modal/form-modal.component';
import { GrafanaComponent } from './grafana/grafana.component';
import { HelperComponent } from './helper/helper.component';
import { LanguageSelectorComponent } from './language-selector/language-selector.component';
import { LoadingPanelComponent } from './loading-panel/loading-panel.component';
import { ModalComponent } from './modal/modal.component';
import { NotificationsSidebarComponent } from './notifications-sidebar/notifications-sidebar.component';
import { OrchestratorDocModalComponent } from './orchestrator-doc-modal/orchestrator-doc-modal.component';
import { OrchestratorDocPanelComponent } from './orchestrator-doc-panel/orchestrator-doc-panel.component';
import { PwdExpirationNotificationComponent } from './pwd-expiration-notification/pwd-expiration-notification.component';
import { RefreshSelectorComponent } from './refresh-selector/refresh-selector.component';
import { SelectBadgesComponent } from './select-badges/select-badges.component';
import { SelectComponent } from './select/select.component';
import { SparklineComponent } from './sparkline/sparkline.component';
import { SubmitButtonComponent } from './submit-button/submit-button.component';
import { UsageBarComponent } from './usage-bar/usage-bar.component';
import { ViewCacheComponent } from './view-cache/view-cache.component';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    AlertModule.forRoot(),
    NgbPopoverModule,
    ProgressbarModule.forRoot(),
    TooltipModule.forRoot(),
    ChartsModule,
    ReactiveFormsModule,
    PipesModule,
    ModalModule.forRoot(),
    DirectivesModule,
    BsDropdownModule,
    NgBootstrapFormValidationModule,
    ClickOutsideModule,
    SimplebarAngularModule,
    RouterModule
  ],
  declarations: [
    ViewCacheComponent,
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
    OrchestratorDocPanelComponent,
    OrchestratorDocModalComponent
  ],
  providers: [],
  exports: [
    ViewCacheComponent,
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
    OrchestratorDocPanelComponent
  ],
  entryComponents: [
    ModalComponent,
    CriticalConfirmationModalComponent,
    ConfirmationModalComponent,
    FormModalComponent
  ]
})
export class ComponentsModule {}
