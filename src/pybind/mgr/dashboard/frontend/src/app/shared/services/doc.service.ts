import { Injectable } from '@angular/core';

import { BehaviorSubject, Subscription } from 'rxjs';
import { filter, first, map } from 'rxjs/operators';

import { CephReleaseNamePipe } from '../pipes/ceph-release-name.pipe';
import { SummaryService } from './summary.service';

@Injectable({
  providedIn: 'root'
})
export class DocService {
  private releaseDataSource = new BehaviorSubject<string>(null);
  releaseData$ = this.releaseDataSource.asObservable();

  constructor(
    private summaryservice: SummaryService,
    private cephReleaseNamePipe: CephReleaseNamePipe
  ) {
    this.summaryservice.subscribeOnce((summary) => {
      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.releaseDataSource.next(releaseName);
    });
  }

  urlGenerator(release: string, section: string): string {
    const domain = `https://documentation.suse.com/ses/7/single-html/`;

    const sections = {
      iscsi: `${domain}ses-admin/#dashboard-iscsi-management`,
      prometheus: `${domain}ses-deployment/#deploy-cephadm-day2-service-monitoring`,
      'nfs-ganesha': `${domain}ses-admin/#ceph-nfsganesha-config`,
      'rgw-nfs': `${domain}ses-admin/#ceph-nfsganesha-config-service-rgw`,
      rgw: `${domain}ses-admin/#dashboard-ogw-enabling`,
      dashboard: `${domain}ses-admin/#dashboard-initial-configuration`,
      grafana: `${domain}ses-deployment/#deploy-cephadm-day2-service-monitoring`,
      orch: `${domain}ses-deployment/#deploy-cephadm-day2-orch`,
      pgs: `${domain}ses-admin/#ceph-pools`,
      unsued: `${release}`
    };

    return sections[section];
  }

  subscribeOnce(
    section: string,
    next: (release: string) => void,
    error?: (error: any) => void
  ): Subscription {
    return this.releaseData$
      .pipe(
        filter((value) => !!value),
        map((release) => this.urlGenerator(release, section)),
        first()
      )
      .subscribe(next, error);
  }
}
