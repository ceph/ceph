import { Directive, TemplateRef } from '@angular/core';

@Directive({
  selector: '[cdTableDetail]'
})
export class TableDetailDirective {
  constructor(public template?: TemplateRef<any>) {}
}
