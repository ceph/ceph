import { Component, Input, ViewEncapsulation } from '@angular/core';

/**
 * Page header component inspired by the Carbon Design System Page Header react component.
 * @see https://ibm-products.carbondesignsystem.com/?path=/docs/components-pageheader--overview
 *
 * Usage:
 * <cd-page-header title="Page title" description="Optional description">
 * </cd-page-header>
 */
@Component({
  selector: 'cd-page-header',
  templateUrl: './page-header.component.html',
  styleUrls: ['./page-header.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class PageHeaderComponent {
  @Input({ required: true }) title: string;
  @Input() description: string = '';
}
