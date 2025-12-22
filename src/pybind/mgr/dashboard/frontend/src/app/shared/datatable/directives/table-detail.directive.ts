import { Directive, TemplateRef } from '@angular/core';

@Directive({
  selector: '[cdTableDetail]',
  standalone: false
})
export class TableDetailDirective {
  constructor(public template?: TemplateRef<any>) {}
}
