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
  styleUrls: ['./alert-panel.component.scss']
})
export class AlertPanelComponent implements OnInit {
  @ViewChild('content', { static: true })
  alertContent: TemplateRef<any>;
  @ViewChild('actionTpl', { static: true })
  actionTpl: TemplateRef<any>;

  @Input()
  title = '';
  @Input()
  type: 'warning' | 'error' | 'info' | 'success' | 'danger';
  @Input()
  showTitle = true;
  @Input()
  size: 'slim' | 'normal' = 'normal';
  @Input()
  dismissible = false;
  @Input()
  spacingClass = '';
  @Input()
  actionName = '';
  @Input()
  lowContrast = true;

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
        this.title = this.title || $localize`Warning`;
        break;
      case 'error':
        this.title = this.title || $localize`Error`;
        break;
      case 'info':
        this.title = this.title || $localize`Information`;
        break;
      case 'success':
        this.title = this.title || $localize`Success`;
        break;
      case 'danger':
        this.title = this.title || $localize`Danger`;
        break;
    }

    this.notificationContent = {
      type: type,
      template: this.alertContent,
      actionsTemplate: this.actionTpl,
      showClose: this.dismissible,
      title: this.showTitle ? this.title : '',
      lowContrast: this.lowContrast
    };
  }

  onClose(): void {
    this.dismissed.emit();
  }

  onAction(): void {
    this.action.emit();
  }
}
