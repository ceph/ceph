import { NgModule } from '@angular/core';

import { AlertPanelComponent } from '../components/alert-panel/alert-panel.component';
import { LoadingPanelComponent } from '../components/loading-panel/loading-panel.component';
import { AutofocusDirective } from './autofocus.directive';
import { Copy2ClipboardButtonDirective } from './copy2clipboard-button.directive';
import { DimlessBinaryPerSecondDirective } from './dimless-binary-per-second.directive';
import { DimlessBinaryDirective } from './dimless-binary.directive';
import { FormLoadingDirective } from './form-loading.directive';
import { IopsDirective } from './iops.directive';
import { MillisecondsDirective } from './milliseconds.directive';
import { PasswordButtonDirective } from './password-button.directive';
import { TrimDirective } from './trim.directive';

@NgModule({
  imports: [],
  declarations: [
    AutofocusDirective,
    Copy2ClipboardButtonDirective,
    DimlessBinaryDirective,
    DimlessBinaryPerSecondDirective,
    PasswordButtonDirective,
    TrimDirective,
    MillisecondsDirective,
    IopsDirective,
    FormLoadingDirective
  ],
  exports: [
    AutofocusDirective,
    Copy2ClipboardButtonDirective,
    DimlessBinaryDirective,
    DimlessBinaryPerSecondDirective,
    PasswordButtonDirective,
    TrimDirective,
    MillisecondsDirective,
    IopsDirective,
    FormLoadingDirective
  ],
  providers: [],
  entryComponents: [LoadingPanelComponent, AlertPanelComponent]
})
export class DirectivesModule {}
