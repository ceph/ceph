import { Component, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'cd-notification-footer',
  template: `
    <cds-grid>
      <cds-row>
        <cds-col>
          <a cdsLink href="#" (click)="viewAll.emit(); $event.preventDefault()">View All</a>
        </cds-col>
        <cds-col class="text-end">
          <button cdsButton="ghost" size="sm" (click)="settings.emit()">
            <svg cdsIcon="settings" size="16"></svg>
          </button>
        </cds-col>
      </cds-row>
    </cds-grid>
  `,
  styles: []
})
export class NotificationFooterComponent {
  @Output() viewAll = new EventEmitter<void>();
  @Output() settings = new EventEmitter<void>();
} 