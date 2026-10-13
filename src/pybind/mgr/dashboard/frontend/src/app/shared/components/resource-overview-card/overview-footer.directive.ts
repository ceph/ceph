import { Directive } from '@angular/core';

/* Marks projected content for the resource overview card footer. */
@Directive({
  selector: '[cdOverviewFooter]',
  standalone: false
})
export class OverviewFooterDirective {}
