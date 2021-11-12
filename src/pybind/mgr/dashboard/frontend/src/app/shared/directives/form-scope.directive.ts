import { Directive, Input } from '@angular/core';

@Directive({
  selector: '[cdFormScope]'
})
export class FormScopeDirective {
  @Input() cdFormScope: any;
}
