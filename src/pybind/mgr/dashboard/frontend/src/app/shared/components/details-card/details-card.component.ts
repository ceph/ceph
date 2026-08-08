import { Component, Input, TemplateRef } from '@angular/core';

type DetailValue = any;
type DetailTemplateContext = { $implicit: DetailValue; detail: DetailItem };

export interface DetailItem {
  label: string;
  value: DetailValue;
  type?: 'text' | 'status' | 'custom';
  statusIcon?: 'success' | 'error' | 'warning' | 'notification';
  customTemplate?: TemplateRef<DetailTemplateContext>;
  hidden?: boolean;
}

@Component({
  selector: 'cd-details-card',
  templateUrl: './details-card.component.html',
  styleUrls: ['./details-card.component.scss'],
  standalone: false
})
export class DetailsCardComponent {
  @Input()
  cardTitle?: string;

  @Input()
  details?: DetailItem[] = [];

  @Input()
  columns = 4;

  getVisibleDetails(): DetailItem[] {
    return (this.details || []).filter((detail) => !detail.hidden);
  }

  getDetailContext(detail: DetailItem): DetailTemplateContext {
    return {
      $implicit: detail.value,
      detail
    };
  }

  getStatusIcon(detail: DetailItem): 'success' | 'error' | 'warning' | 'notification' {
    if (detail.statusIcon) {
      return detail.statusIcon;
    }
    return this.isStatusDisabled(detail.value) ? 'error' : 'success';
  }

  hasValue(value: DetailValue): boolean {
    return value !== null && value !== undefined && value !== '';
  }

  getDisplayValue(value: DetailValue): DetailValue | string {
    return this.hasValue(value) ? value : '-';
  }

  isStatusDisabled(value: DetailValue): boolean {
    if (value === null || value === undefined) {
      return false;
    }
    const normalized = String(value).trim().toLowerCase();
    return normalized === 'disabled';
  }
}
