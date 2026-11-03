import {
  Component,
  EventEmitter,
  Input,
  OnInit,
  Output,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { NotificationContent, NotificationType } from 'carbon-components-angular';

import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-alert-panel',
  templateUrl: './alert-panel.component.html',
  styleUrls: ['./alert-panel.component.scss'],
  standalone: false
})
export class AlertPanelComponent implements OnInit {
  @ViewChild('content', { static: true })
  alertContent: TemplateRef<any>;
  @ViewChild('actionTpl', { static: true })
  actionTpl: TemplateRef<any>;

  @Input()
  alertTitle = '';
  @Input()
  type: 'warning' | 'error' | 'info' | 'success' | 'danger';
  @Input()
  showTitle = true;
  @Input()
  dismissible = false;
  @Input()
  spacingClass = '';
  @Input()
  actionName = '';
  @Input()
  lowContrast = true;
  @Input()
  variant: 'toast' | 'inline' = 'inline';

  /**
   * The event that is triggered when the close button (x) has been
   * pressed.
   */
  @Output()
  dismissed = new EventEmitter();

  /**
   * The event that is triggered when the action button has been
   * pressed.
   */
  @Output()
  action = new EventEmitter();

  icons = Icons;

  notificationContent: NotificationContent;

  ngOnInit() {
    const type: NotificationType = this.type === 'danger' ? 'error' : this.type;
    switch (this.type) {
      case 'warning':
        this.alertTitle = this.alertTitle || $localize`Warning`;
        break;
      case 'error':
        this.alertTitle = this.alertTitle || $localize`Error`;
        break;
      case 'info':
        this.alertTitle = this.alertTitle || $localize`Information`;
        break;
      case 'success':
        this.alertTitle = this.alertTitle || $localize`Success`;
        break;
      case 'danger':
        this.alertTitle = this.alertTitle || $localize`Danger`;
        break;
    }

    this.notificationContent = {
      type: type,
      template: this.alertContent,
      actionsTemplate: this.actionTpl,
      showClose: this.dismissible,
      title: this.showTitle ? this.alertTitle : '',
      lowContrast: this.lowContrast,
      variant: this.variant
    };
  }

  onClose(): void {
    this.dismissed.emit();
  }

  onAction(): void {
    this.action.emit();
  }
}
