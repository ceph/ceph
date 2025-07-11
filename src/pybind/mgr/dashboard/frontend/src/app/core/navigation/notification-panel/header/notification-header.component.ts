import { Component, Output, EventEmitter, OnInit, OnDestroy } from '@angular/core';
import { NotificationService } from '../../../../shared/services/notification.service';
import { Subscription } from 'rxjs';

@Component({
  selector: 'cd-notification-header',
  templateUrl: './notification-header.component.html',
  styleUrls: ['./notification-header.component.scss']
})
export class NotificationHeaderComponent implements OnInit, OnDestroy {
  @Output() dismissAll = new EventEmitter<void>();

  isMuted = false;
  private subs = new Subscription();

  constructor(private notificationService: NotificationService) {}

  ngOnInit(): void {
    this.subs.add(
      this.notificationService.muteState$.subscribe((isMuted) => {
        this.isMuted = isMuted;
      })
    );
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  onDismissAll(): void {
    this.dismissAll.emit();
    this.notificationService.removeAll();
  }

  onToggleMute(): void {
    this.notificationService.suspendToasties(!this.isMuted);
  }
}
