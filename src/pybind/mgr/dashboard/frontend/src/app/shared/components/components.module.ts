import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { AlertModule } from 'ngx-bootstrap/alert';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ModalModule } from 'ngx-bootstrap/modal';
import { PopoverModule } from 'ngx-bootstrap/popover';
import { TooltipModule } from 'ngx-bootstrap/tooltip';

import { DirectivesModule } from '../directives/directives.module';
import { PipesModule } from '../pipes/pipes.module';
import { BackButtonComponent } from './back-button/back-button.component';
import { ConfirmationModalComponent } from './confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from './critical-confirmation-modal/critical-confirmation-modal.component';
import { ErrorPanelComponent } from './error-panel/error-panel.component';
import { GrafanaComponent } from './grafana/grafana.component';
import { HelperComponent } from './helper/helper.component';
import { InfoPanelComponent } from './info-panel/info-panel.component';
import { LanguageSelectorComponent } from './language-selector/language-selector.component';
import { LoadingPanelComponent } from './loading-panel/loading-panel.component';
import { ModalComponent } from './modal/modal.component';
import { RefreshSelectorComponent } from './refresh-selector/refresh-selector.component';
import { SelectBadgesComponent } from './select-badges/select-badges.component';
import { SelectComponent } from './select/select.component';
import { SparklineComponent } from './sparkline/sparkline.component';
import { SubmitButtonComponent } from './submit-button/submit-button.component';
import { UsageBarComponent } from './usage-bar/usage-bar.component';
import { ViewCacheComponent } from './view-cache/view-cache.component';
import { WarningPanelComponent } from './warning-panel/warning-panel.component';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    AlertModule.forRoot(),
    PopoverModule.forRoot(),
    TooltipModule.forRoot(),
    ChartsModule,
    ReactiveFormsModule,
    PipesModule,
    ModalModule.forRoot(),
    DirectivesModule,
    BsDropdownModule
  ],
  declarations: [
    ViewCacheComponent,
    SparklineComponent,
    HelperComponent,
    SelectBadgesComponent,
    SubmitButtonComponent,
    UsageBarComponent,
    ErrorPanelComponent,
    LoadingPanelComponent,
    InfoPanelComponent,
    ModalComponent,
    CriticalConfirmationModalComponent,
    ConfirmationModalComponent,
    WarningPanelComponent,
    LanguageSelectorComponent,
    GrafanaComponent,
    SelectComponent,
    BackButtonComponent,
    RefreshSelectorComponent
  ],
  providers: [],
  exports: [
    ViewCacheComponent,
    SparklineComponent,
    HelperComponent,
    SelectBadgesComponent,
    SubmitButtonComponent,
    BackButtonComponent,
    ErrorPanelComponent,
    LoadingPanelComponent,
    InfoPanelComponent,
    UsageBarComponent,
    ModalComponent,
    WarningPanelComponent,
    LanguageSelectorComponent,
    GrafanaComponent,
    SelectComponent,
    RefreshSelectorComponent
  ],
  entryComponents: [ModalComponent, CriticalConfirmationModalComponent, ConfirmationModalComponent]
})
export class ComponentsModule {}
