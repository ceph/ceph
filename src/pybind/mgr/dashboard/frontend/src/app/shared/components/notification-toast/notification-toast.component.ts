import { Component, OnDestroy, OnInit } from '@angular/core';
import { animate, style, transition, trigger } from '@angular/animations';
import { Subscription } from 'rxjs';
import { ToastContent } from 'carbon-components-angular';
import { NotificationService } from '../../services/notification.service';

@Component({
  selector: 'cd-toast',
  templateUrl: './notification-toast.component.html',
  styleUrls: ['./notification-toast.component.scss'],
  animations: [
    trigger('toastAnimation', [
      transition(
        ':enter',
        [
          style({ opacity: 0, transform: 'translateX(100%)' }),
          animate('{{duration}} {{easing}}', style({ opacity: 1, transform: 'translateX(0)' }))
        ],
        { params: { duration: '240ms', easing: 'cubic-bezier(0.2, 0, 0.38, 0.9)' } }
      ),
      transition(
        ':leave',
        [
          style({ opacity: 1, transform: 'translateX(0)' }),
          animate('{{duration}} {{easing}}', style({ opacity: 0, transform: 'translateX(100%)' }))
        ],
        { params: { duration: '240ms', easing: 'cubic-bezier(0.2, 0, 0.38, 0.9)' } }
      )
    ])
  ]
})
export class ToastComponent implements OnInit, OnDestroy {
  activeToasts: ToastContent[] = [];
  private subscription: Subscription;

  constructor(private notificationService: NotificationService) {}

  ngOnInit() {
    this.subscription = this.notificationService.activeToasts$.subscribe(
      (toasts: ToastContent[]) => {
        this.activeToasts = toasts;
      }
    );
  }

  ngOnDestroy() {
    if (this.subscription) {
      this.subscription.unsubscribe();
    }
  }

  onToastClose(toast: ToastContent) {
    this.notificationService.removeToast(toast);
  }
}
