import { NgModule } from '@angular/core';

import { AutofocusDirective } from './autofocus.directive';
import { Copy2ClipboardButtonDirective } from './copy2clipboard-button.directive';
import { DimlessBinaryPerSecondDirective } from './dimless-binary-per-second.directive';
import { DimlessBinaryDirective } from './dimless-binary.directive';
import { IopsDirective } from './iops.directive';
import { MillisecondsDirective } from './milliseconds.directive';
import { PasswordButtonDirective } from './password-button.directive';
import { TextToDownloadDirective } from './text-to-download.directive';
import { ToClipboardDirective } from './to-clipboard.directive';

@NgModule({
  imports: [],
  declarations: [
    AutofocusDirective,
    Copy2ClipboardButtonDirective,
    DimlessBinaryDirective,
    DimlessBinaryPerSecondDirective,
    PasswordButtonDirective,
    MillisecondsDirective,
    IopsDirective,
    TextToDownloadDirective,
    ToClipboardDirective
  ],
  exports: [
    AutofocusDirective,
    Copy2ClipboardButtonDirective,
    DimlessBinaryDirective,
    DimlessBinaryPerSecondDirective,
    PasswordButtonDirective,
    MillisecondsDirective,
    IopsDirective,
    ToClipboardDirective,
    TextToDownloadDirective
  ],
  providers: []
})
export class DirectivesModule {}
