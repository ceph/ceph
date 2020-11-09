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

  urlGenerator(section: string, release = '7p'): string {
    const domain = `https://documentation.suse.com/ses/${release}/single-html/`;
    const sections = {
      iscsi: `${domain}ses-admin/#dashboard-iscsi-management`,
      prometheus: `${domain}ses-deployment/#deploy-cephadm-day2-service-monitoring`,
      'nfs-ganesha': `${domain}ses-admin/#cha-ceph-nfsganesha`,
      'rgw-nfs': `${domain}ses-admin/#ganesha-rgw-supported-operations`,
      rgw: `${domain}ses-admin/#dashboard-ogw-enabling`,
      dashboard: `${domain}ses-admin/#dashboard-initial-configuration`,
      grafana: `${domain}ses-deployment/#deploy-cephadm-day2-service-monitoring`,
      orch: `${domain}ses-deployment/#deploy-cephadm-day2-orch`,
      pgs: `${domain}ses-admin/#ceph-pools`,
      help: `${domain}`,
      security: `${domain}ses-security`,
      trademarks: `${domain}`,
      'dashboard-landing-page-status': `${domain}ses-admin/#dashboard-widgets-status`,
      'dashboard-landing-page-performance': `${domain}ses-admin/#dashboard-widgets-performance`,
      'dashboard-landing-page-capacity': `${domain}ses-admin/#dashboard-widgets-capacity`
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
        map(() => this.urlGenerator(section)),
        first()
      )
      .subscribe(next, error);
  }
}
