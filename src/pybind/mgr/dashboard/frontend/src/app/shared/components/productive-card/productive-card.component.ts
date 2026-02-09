import { CommonModule } from '@angular/common';
import { Component, ContentChild, Input, TemplateRef } from '@angular/core';
import { GridModule, LayerModule, TilesModule } from 'carbon-components-angular';

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
  imports: [GridModule, TilesModule, CommonModule, LayerModule],
  standalone: true,
  templateUrl: './productive-card.component.html',
  styleUrl: './productive-card.component.scss'
})
export class ProductiveCardComponent {
  /* Card Title */
  @Input() headerTitle!: string;

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
