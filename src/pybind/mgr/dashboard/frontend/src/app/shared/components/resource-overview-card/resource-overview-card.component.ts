import { Component, Input } from '@angular/core';

export type OverviewValue = string | number | boolean | null | undefined;

export interface OverviewField {
  /* Human-readable label shown for the field. */
  label: string;
  /* Single value rendered for text/status fields. */
  value?: OverviewValue;
  /* Multiple values rendered when the field uses tag display. */
  values?: OverviewValue[];
  /* Selects how the field value should be presented in the UI. */
  type?: 'text' | 'status' | 'tags';
  /* Visual tone used by status rendering (icon/text styling). */
  status?: 'success' | 'warning' | 'danger' | 'info-circle';
  /* Fallback text shown when the value is empty. */
  emptyText?: string;
}

@Component({
  selector: 'cd-resource-overview-card',
  templateUrl: './resource-overview-card.component.html',
  styleUrls: ['./resource-overview-card.component.scss'],
  standalone: false
})
export class OverviewComponent {
  /* Title shown at the top of the overview card. */
  @Input() title = '';
  /* Fields rendered in the overview card. */
  @Input() fields: OverviewField[] = [];
  /* Number of columns used to layout the fields. */
  @Input() columns = 3;
}
