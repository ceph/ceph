import { Component, OnInit } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, ignoreElements, tap } from 'rxjs/operators';
import { HealthService } from '~/app/shared/api/health.service';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { Permission } from '~/app/shared/models/permissions';
import { UpgradeInfoInterface } from '~/app/shared/models/upgrade.interface';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
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
  upgradeInfo$: Observable<UpgradeInfoInterface>;
  upgradeInfoError$: Observable<any>;
  permission: Permission;
  healthData$: Observable<any>;
  fsid$: Observable<any>;
  modalRef: NgbModalRef;
  upgradableVersions: string[];

  icons = Icons;

  constructor(
    private modalService: ModalService,
    private summaryService: SummaryService,
    private upgradeService: UpgradeService,
    private authStorageService: AuthStorageService,
    private healthService: HealthService
  ) {
    this.permission = this.authStorageService.getPermissions().configOpt;
  }

  ngOnInit(): void {
    this.summaryService.subscribe((summary) => {
      const version = summary.version.replace('ceph version ', '').split('-');
      this.version = version[0];
    });
    this.upgradeInfo$ = this.upgradeService
      .list()
      .pipe(
        tap((upgradeInfo: UpgradeInfoInterface) => (this.upgradableVersions = upgradeInfo.versions))
      );
    this.upgradeInfoError$ = this.upgradeInfo$?.pipe(
      ignoreElements(),
      catchError((error) => of(error))
    );
    this.healthData$ = this.healthService.getMinimalHealth();
    this.fsid$ = this.healthService.getClusterFsid();
  }

  startUpgradeModal() {
    this.modalRef = this.modalService.show(UpgradeStartModalComponent, {
      versions: this.upgradableVersions.sort()
    });
  }
}
