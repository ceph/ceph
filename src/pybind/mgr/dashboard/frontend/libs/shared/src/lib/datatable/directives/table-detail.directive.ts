import { Directive, TemplateRef } from '@angular/core';

@Directive({
    selector: '[cdTableDetail]',
    standalone: true,
})
export class TableDetailDirective {
  constructor(public template?: TemplateRef<any>) {}
}
