import {Directive, Input, ViewContainerRef} from '@angular/core';

@Directive({
  selector: '[cdTableDetails]'
})
export class TableDetailsDirective {
  @Input() selected?: any[];

  constructor(public viewContainerRef: ViewContainerRef) { }

}
