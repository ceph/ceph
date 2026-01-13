import { Component, ContentChild, Input, TemplateRef } from '@angular/core';

/**
 * A generic productive card component.
 *
 * @example
 * <cd-productive-card title="Card Title"
 *                     [applyShadow]="true">
 *   <ng-template #headerAction>...</ng-template>
 *   <ng-template #footer>...</ng-template>
 *   <p>My card body content</p>
 * </cd-productive-card>
 */
@Component({
  selector: 'cd-productive-card',
  standalone: false,
  templateUrl: './productive-card.component.html',
  styleUrl: './productive-card.component.scss'
})
export class ProductiveCardComponent {
  /* Card Title */
  @Input() title!: string;

  /* Optional: Applies a tinted-colored background to card */
  @Input() applyShadow: boolean = false;

  /* Optional: Header action template, appears alongwith title in top-right corner */
  @ContentChild('headerAction', {
    read: TemplateRef
  })
  headerActionTemplate?: TemplateRef<any>;

  /* Optional: Footer template , otherwise no footer will be used for card.*/
  @ContentChild('footer', {
    read: TemplateRef
  })
  footerTemplate?: TemplateRef<any>;
}
