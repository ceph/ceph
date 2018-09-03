import { NgModule } from '@angular/core';

import { AutofocusDirective } from './autofocus.directive';
import { Copy2ClipboardButtonDirective } from './copy2clipboard-button.directive';
import { DimlessBinaryDirective } from './dimless-binary.directive';
import { PasswordButtonDirective } from './password-button.directive';

@NgModule({
  imports: [],
  declarations: [
    AutofocusDirective,
    Copy2ClipboardButtonDirective,
    DimlessBinaryDirective,
    PasswordButtonDirective
  ],
  exports: [
    AutofocusDirective,
    Copy2ClipboardButtonDirective,
    DimlessBinaryDirective,
    PasswordButtonDirective
  ],
  providers: []
})
export class DirectivesModule {}
