import { Directive, Input } from '@angular/core';

@Directive({
  selector: '[cdFormScope]',
  standalone: false
})
export class FormScopeDirective {
  @Input() cdFormScope: any;
}
