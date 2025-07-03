import { Component, OnDestroy, OnInit } from '@angular/core';
import { Subscription } from 'rxjs';
import { ToastContent } from 'carbon-components-angular';
import { NotificationService } from '../../services/notification.service';

@Component({
  selector: 'cd-toast',
  templateUrl: './notification-toast.component.html',
  styleUrls: ['./notification-toast.component.scss']
})
export class Toast implements OnInit, OnDestroy {
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