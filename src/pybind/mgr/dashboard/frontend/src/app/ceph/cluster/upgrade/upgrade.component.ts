import { Component, OnInit } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, publishReplay, refCount, tap } from 'rxjs/operators';
import { DaemonService } from '~/app/shared/api/daemon.service';
import { HealthService } from '~/app/shared/api/health.service';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { Daemon } from '~/app/shared/models/daemon.interface';
import { Permission } from '~/app/shared/models/permissions';
import { UpgradeInfoInterface } from '~/app/shared/models/upgrade.interface';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { UpgradeStartModalComponent } from './upgrade-form/upgrade-start-modal.component';

@Component({
  selector: 'cd-upgrade',
  templateUrl: './upgrade.component.html',
  styleUrls: ['./upgrade.component.scss']
})
export class UpgradeComponent implements OnInit {
  version: string;
  info$: Observable<UpgradeInfoInterface>;
  permission: Permission;
  healthData$: Observable<any>;
  daemons$: Observable<Daemon[]>;
  fsid$: Observable<any>;
  modalRef: NgbModalRef;
  upgradableVersions: string[];
  errorMessage: string;

  columns: CdTableColumn[] = [];

  icons = Icons;

  constructor(
    private modalService: ModalService,
    private summaryService: SummaryService,
    private upgradeService: UpgradeService,
    private healthService: HealthService,
    private daemonService: DaemonService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Daemon name`,
        prop: 'daemon_name',
        flexGrow: 1,
        filterable: true
      },
      {
        name: $localize`Version`,
        prop: 'version',
        flexGrow: 1,
        filterable: true
      }
    ];

    this.summaryService.subscribe((summary) => {
      const version = summary.version.replace('ceph version ', '').split('-');
      this.version = version[0];
    });
    this.info$ = this.upgradeService.list().pipe(
      tap((upgradeInfo: UpgradeInfoInterface) => (this.upgradableVersions = upgradeInfo.versions)),
      publishReplay(1),
      refCount(),
      catchError((err) => {
        err.preventDefault();
        this.errorMessage = $localize`Not retrieving upgrades`;
        this.notificationService.show(
          NotificationType.error,
          this.errorMessage,
          err.error.detail || err.error.message
        );
        return of(null);
      })
    );
    this.healthData$ = this.healthService.getMinimalHealth();
    this.daemons$ = this.daemonService.list(this.upgradeService.upgradableServiceTypes);
    this.fsid$ = this.healthService.getClusterFsid();
  }

  startUpgradeModal() {
    this.modalRef = this.modalService.show(UpgradeStartModalComponent, {
      versions: this.upgradableVersions
    });
  }

  upgradeNow(version: string) {
    this.upgradeService.start(version).subscribe({
      error: (error) => {
        this.notificationService.show(
          NotificationType.error,
          $localize`Failed to start the upgrade`,
          error
        );
      },
      complete: () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Started upgrading the cluster`
        );
      }
    });
  }
}
