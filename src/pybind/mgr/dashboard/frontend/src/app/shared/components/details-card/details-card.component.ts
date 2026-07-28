import { Component, EventEmitter, Input, Output, TemplateRef } from '@angular/core';

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
  showEditButton = false;

  @Input()
  editButtonLabel = $localize`Edit`;

  @Input()
  editButtonDisabled = false;

  @Input()
  columns = 4;

  @Output()
  editClicked = new EventEmitter<void>();

  onEditClick(): void {
    this.editClicked.emit();
  }
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
