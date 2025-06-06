import { Component, Input, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'cd-notification-header',
  template: `
    <cds-header name="Notifications" class="cds--g10">
      <cds-header-global>
        <cds-header-action>
          <svg cdsIcon="notification--filled" size="20"></svg>
          <cds-tag type="blue" *ngIf="unreadCount > 0">{{unreadCount}}</cds-tag>
        </cds-header-action>
        <cds-header-action>
          <cds-toggle
            [label]="'Do Not Disturb'"
            [hideLabel]="false"
            [checked]="doNotDisturb"
            (checkedChange)="doNotDisturbChange.emit($event)">
          </cds-toggle>
        </cds-header-action>
        <cds-header-action>
          <button cdsButton="ghost" (click)="close.emit()">
            <svg cdsIcon="close" size="20"></svg>
          </button>
        </cds-header-action>
      </cds-header-global>
    </cds-header>
  `,
  styles: [`
    :host {
      display: block;
      background-color: var(--cds-layer-01);
    }

    cds-header {
      border-bottom: 1px solid var(--cds-border-subtle);
    }

    cds-tag {
      margin-left: 0.5rem;
    }

    cds-toggle {
      margin-right: 1rem;
    }
  `]
})
export class NotificationHeaderComponent {
  @Input() unreadCount = 0;
  @Input() doNotDisturb = false;
  @Output() doNotDisturbChange = new EventEmitter<boolean>();
  @Output() close = new EventEmitter<void>();
} 