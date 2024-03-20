import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ApiClient } from './api-client';
import { map, shareReplay, tap } from 'rxjs/operators';
import { SummaryService } from '../services/summary.service';
import { UpgradeInfoInterface, UpgradeStatusInterface } from '../models/upgrade.interface';
import { Observable } from 'rxjs';
import { UpgradeStartModalComponent } from '~/app/ceph/cluster/upgrade/upgrade-form/upgrade-start-modal.component';
import { ModalService } from '../services/modal.service';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

const CACHE_SIZE = 1;

@Injectable({
  providedIn: 'root'
})
export class UpgradeService extends ApiClient {
  baseURL = 'api/cluster/upgrade';

  upgradableServiceTypes = [
    'mgr',
    'mon',
    'crash',
    'osd',
    'mds',
    'rgw',
    'rbd-mirror',
    'cephfs-mirror',
    'iscsi',
    'nfs'
  ];

  _listData$: Observable<UpgradeInfoInterface>;
  _upgradableVersions: string[];

  constructor(
    private http: HttpClient,
    private summaryService: SummaryService,
    private modalService: ModalService
  ) {
    super();
  }

  list() {
    return this.http.get(this.baseURL).pipe(
      map((resp: UpgradeInfoInterface) => {
        return this.versionAvailableForUpgrades(resp);
      })
    );
  }

  // Filter out versions that are older than the current cluster version
  // Only allow upgrades to the same major version
  versionAvailableForUpgrades(upgradeInfo: UpgradeInfoInterface): UpgradeInfoInterface {
    let version = '';
    this.summaryService.subscribe((summary) => {
      version = summary.version.replace('ceph version ', '').split('-')[0];
    });

    const upgradableVersions = upgradeInfo.versions.filter((targetVersion) => {
      const cVersion = version.split('.');
      const tVersion = targetVersion.split('.');
      return (
        cVersion[0] === tVersion[0] && (cVersion[1] < tVersion[1] || cVersion[2] < tVersion[2])
      );
    });
    upgradeInfo.versions = upgradableVersions.sort();
    return upgradeInfo;
  }

  start(version?: string, image?: string) {
    return this.http.post(`${this.baseURL}/start`, { image: image, version: version });
  }

  pause() {
    return this.http.put(`${this.baseURL}/pause`, null);
  }

  resume() {
    return this.http.put(`${this.baseURL}/resume`, null);
  }

  stop() {
    return this.http.put(`${this.baseURL}/stop`, null);
  }

  status(): Observable<UpgradeStatusInterface> {
    return this.http.get<UpgradeStatusInterface>(`${this.baseURL}/status`);
  }

  listCached(): Observable<UpgradeInfoInterface> {
    if (!this._listData$) {
      this._listData$ = this.list().pipe(
        tap(
          (upgradeInfo: UpgradeInfoInterface) => (this._upgradableVersions = upgradeInfo.versions)
        ),
        shareReplay(CACHE_SIZE)
      );
    }
    return this._listData$;
  }

  startUpgradeModal(): NgbModalRef {
    return this.modalService.show(UpgradeStartModalComponent, {
      versions: this._upgradableVersions
    });
  }
}
