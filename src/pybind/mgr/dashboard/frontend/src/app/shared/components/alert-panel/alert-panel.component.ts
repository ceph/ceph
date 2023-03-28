import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';

import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-alert-panel',
  templateUrl: './alert-panel.component.html',
  styleUrls: ['./alert-panel.component.scss']
})
export class AlertPanelComponent implements OnInit {
  @Input()
  title = '';
  @Input()
  bootstrapClass = '';
  @Input()
  type: 'warning' | 'error' | 'info' | 'success' | 'danger';
  @Input()
  typeIcon: Icons | string;
  @Input()
  size: 'slim' | 'normal' = 'normal';
  @Input()
  showIcon = true;
  @Input()
  showTitle = true;
  @Input()
  dismissible = false;
  @Input()
  spacingClass = '';

  /**
   * The event that is triggered when the close button (x) has been
   * pressed.
   */
  @Output()
  dismissed = new EventEmitter();

  icons = Icons;

  ngOnInit() {
    switch (this.type) {
      case 'warning':
        this.title = this.title || $localize`Warning`;
        this.typeIcon = this.typeIcon || Icons.warning;
        this.bootstrapClass = this.bootstrapClass || 'warning';
        break;
      case 'error':
        this.title = this.title || $localize`Error`;
        this.typeIcon = this.typeIcon || Icons.destroyCircle;
        this.bootstrapClass = this.bootstrapClass || 'danger';
        break;
      case 'info':
        this.title = this.title || $localize`Information`;
        this.typeIcon = this.typeIcon || Icons.infoCircle;
        this.bootstrapClass = this.bootstrapClass || 'info';
        break;
      case 'success':
        this.title = this.title || $localize`Success`;
        this.typeIcon = this.typeIcon || Icons.check;
        this.bootstrapClass = this.bootstrapClass || 'success';
        break;
      case 'danger':
        this.title = this.title || $localize`Danger`;
        this.typeIcon = this.typeIcon || Icons.warning;
        this.bootstrapClass = this.bootstrapClass || 'danger';
        break;
    }
  }

  onClose(): void {
    this.dismissed.emit();
  }
}
