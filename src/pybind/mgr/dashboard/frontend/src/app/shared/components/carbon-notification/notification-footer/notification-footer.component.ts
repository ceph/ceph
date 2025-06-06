import { Component, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'cd-notification-footer',
  template: `
    <div ibmGrid>
      <div ibmRow>
        <div ibmCol>
          <a cdsLink href="#" (click)="viewAll.emit(); $event.preventDefault()">View All</a>
        </div>
        <div ibmCol class="text-end">
          <button cdsButton="ghost" size="sm" (click)="settings.emit()">
            <svg cdsIcon="settings" size="16"></svg>
          </button>
        </div>
      </div>
    </div>
  `,
  styles: []
})
export class NotificationFooterComponent {
  @Output() viewAll = new EventEmitter<void>();
  @Output() settings = new EventEmitter<void>();
} 