import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { AlertModule, ModalModule, PopoverModule, TooltipModule } from 'ngx-bootstrap';

import { PipesModule } from '../pipes/pipes.module';
import { ConfirmationModalComponent } from './confirmation-modal/confirmation-modal.component';
import { DeletionModalComponent } from './deletion-modal/deletion-modal.component';
import { ErrorPanelComponent } from './error-panel/error-panel.component';
import { HelperComponent } from './helper/helper.component';
import { InfoPanelComponent } from './info-panel/info-panel.component';
import { LoadingPanelComponent } from './loading-panel/loading-panel.component';
import { ModalComponent } from './modal/modal.component';
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
    ModalModule.forRoot()
  ],
  declarations: [
    ViewCacheComponent,
    SparklineComponent,
    HelperComponent,
    SubmitButtonComponent,
    UsageBarComponent,
    ErrorPanelComponent,
    LoadingPanelComponent,
    InfoPanelComponent,
    ModalComponent,
    DeletionModalComponent,
    ConfirmationModalComponent,
    WarningPanelComponent
  ],
  providers: [],
  exports: [
    ViewCacheComponent,
    SparklineComponent,
    HelperComponent,
    SubmitButtonComponent,
    ErrorPanelComponent,
    LoadingPanelComponent,
    InfoPanelComponent,
    UsageBarComponent,
    ModalComponent,
    WarningPanelComponent
  ],
  entryComponents: [ModalComponent, DeletionModalComponent, ConfirmationModalComponent]
})
export class ComponentsModule {}
