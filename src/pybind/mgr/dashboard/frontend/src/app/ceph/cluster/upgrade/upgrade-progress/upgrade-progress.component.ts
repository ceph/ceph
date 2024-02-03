import { Component, OnDestroy, OnInit } from '@angular/core';

import { Observable, ReplaySubject, Subscription } from 'rxjs';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import { Icons } from '~/app/shared/enum/icons.enum';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ModalService } from '~/app/shared/services/modal.service';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { SummaryService } from '~/app/shared/services/summary.service';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { shareReplay, switchMap, tap } from 'rxjs/operators';
import { Router } from '@angular/router';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { UpgradeStatusInterface } from '~/app/shared/models/upgrade.interface';

@Component({
  selector: 'cd-upgrade-progress',
  templateUrl: './upgrade-progress.component.html',
  styleUrls: ['./upgrade-progress.component.scss']
})
export class UpgradeProgressComponent implements OnInit, OnDestroy {
  permission: Permission;
  icons = Icons;
  modalRef: NgbModalRef;
  interval = new Subscription();
  executingTask: ExecutingTask;

  upgradeStatus$: Observable<UpgradeStatusInterface>;
  subject = new ReplaySubject<UpgradeStatusInterface>();

  constructor(
    private authStorageService: AuthStorageService,
    private upgradeService: UpgradeService,
    private notificationService: NotificationService,
    private modalService: ModalService,
    private summaryService: SummaryService,
    private router: Router,
    private refreshIntervalService: RefreshIntervalService
  ) {
    this.permission = this.authStorageService.getPermissions().configOpt;
  }

  ngOnInit() {
    this.upgradeStatus$ = this.subject.pipe(
      switchMap(() => this.upgradeService.status()),
      tap((status: UpgradeStatusInterface) => {
        if (!status.in_progress) {
          this.router.navigate(['/upgrade']);
        }
      }),
      shareReplay(1)
    );

    this.interval = this.refreshIntervalService.intervalData$.subscribe(() => {
      this.fetchStatus();
    });

    this.summaryService.subscribe((summary) => {
      this.executingTask = summary.executing_tasks.filter((tasks) =>
        tasks.name.includes('progress/Upgrade')
      )[0];
    });
  }

  pauseUpgrade() {
    this.upgradeService.pause().subscribe({
      error: (error) => {
        this.notificationService.show(
          NotificationType.error,
          $localize`Failed to pause the upgrade`,
          error
        );
      },
      complete: () => {
        this.notificationService.show(NotificationType.success, $localize`The upgrade is paused`);
        this.fetchStatus();
      }
    });
  }

  fetchStatus() {
    this.subject.next();
  }

  resumeUpgrade(modal = false) {
    this.upgradeService.resume().subscribe({
      error: (error) => {
        this.notificationService.show(
          NotificationType.error,
          $localize`Failed to resume the upgrade`,
          error
        );
      },
      complete: () => {
        this.fetchStatus();
        this.notificationService.show(NotificationType.success, $localize`Upgrade is resumed`);
        if (modal) {
          this.modalRef.close();
        }
      }
    });
  }

  stopUpgradeModal() {
    // pause the upgrade meanwhile we get stop confirmation from user
    this.pauseUpgrade();
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'Upgrade',
      actionDescription: 'stop',
      submitAction: () => this.stopUpgrade(),
      callBackAtionObservable: () => this.resumeUpgrade(true)
    });
  }

  stopUpgrade() {
    this.modalRef.close();
    this.upgradeService.stop().subscribe({
      error: (error) => {
        this.notificationService.show(
          NotificationType.error,
          $localize`Failed to stop the upgrade`,
          error
        );
      },
      complete: () => {
        this.notificationService.show(NotificationType.success, $localize`The upgrade is stopped`);
        this.router.navigate(['/upgrade']);
      }
    });
  }

  ngOnDestroy() {
    this.interval?.unsubscribe();
  }
}
