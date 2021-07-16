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

  urlGenerator(section: string, release = '7'): string {
    const domain = `https://documentation.suse.com/ses/${release}/single-html/`;
    const domainCeph = `https://ceph.io/`;

    const sections = {
      iscsi: `${domain}mgr/dashboard/#enabling-iscsi-management`,
      prometheus: `${domain}mgr/dashboard/#enabling-prometheus-alerting`,
      'nfs-ganesha': `${domain}mgr/dashboard/#configuring-nfs-ganesha-in-the-dashboard`,
      'rgw-nfs': `${domain}radosgw/nfs`,
      rgw: `${domain}mgr/dashboard/#enabling-the-object-gateway-management-frontend`,
      dashboard: `${domain}mgr/dashboard`,
      grafana: `${domain}mgr/dashboard/#enabling-the-embedding-of-grafana-dashboards`,
      orch: `${domain}mgr/orchestrator`,
      pgs: `${domainCeph}pgcalc`,
      help: `${domainCeph}help/`,
      security: `${domainCeph}security/`,
      trademarks: `${domainCeph}legal-page/trademarks/`,
      'dashboard-landing-page-status': `${domain}mgr/dashboard/#dashboard-landing-page-status`,
      'dashboard-landing-page-performance': `${domain}mgr/dashboard/#dashboard-landing-page-performance`,
      'dashboard-landing-page-capacity': `${domain}mgr/dashboard/#dashboard-landing-page-capacity`
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
        map((release) => this.urlGenerator(section, release)),
        first()
      )
      .subscribe(next, error);
  }
}
