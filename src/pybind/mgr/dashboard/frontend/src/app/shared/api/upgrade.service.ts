import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ApiClient } from './api-client';
import { map } from 'rxjs/operators';
import { SummaryService } from '../services/summary.service';
import { UpgradeInfoInterface } from '../models/upgrade.interface';

@Injectable({
  providedIn: 'root'
})
export class UpgradeService extends ApiClient {
  baseURL = 'api/cluster/upgrade';

  constructor(private http: HttpClient, private summaryService: SummaryService) {
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
    upgradeInfo.versions = upgradableVersions;
    return upgradeInfo;
  }

  start(version: string) {
    return this.http.post(`${this.baseURL}/start`, { version: version });
  }
}
