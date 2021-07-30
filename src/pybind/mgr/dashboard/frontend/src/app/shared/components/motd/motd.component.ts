import { Component, OnDestroy, OnInit } from '@angular/core';

import { Subscription } from 'rxjs';

import { Motd } from '../../../shared/api/motd.service';
import { MotdNotificationService } from '../../../shared/services/motd-notification.service';

@Component({
  selector: 'cd-motd',
  templateUrl: './motd.component.html',
  styleUrls: ['./motd.component.scss']
})
export class MotdComponent implements OnInit, OnDestroy {
  motd: Motd | undefined = undefined;

  private subscription: Subscription;

  constructor(private motdNotificationService: MotdNotificationService) {}

  ngOnInit(): void {
    this.subscription = this.motdNotificationService.motd$.subscribe((motd: Motd | undefined) => {
      this.motd = motd;
    });
  }

  ngOnDestroy(): void {
    this.subscription.unsubscribe();
  }

  onDismissed(): void {
    this.motdNotificationService.hide();
  }
}
