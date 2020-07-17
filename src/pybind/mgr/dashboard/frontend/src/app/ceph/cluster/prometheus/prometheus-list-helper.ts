import { OnInit } from '@angular/core';

import { PrometheusService } from '../../../shared/api/prometheus.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { SummaryService } from '../../../shared/services/summary.service';

export class PrometheusListHelper extends ListWithDetails implements OnInit {
  public isPrometheusConfigured = false;
  public isAlertmanagerConfigured = false;
  public docsUrl = '';

  constructor(
    protected prometheusService: PrometheusService,
    protected summaryService: SummaryService,
    protected cephReleaseNamePipe: CephReleaseNamePipe
  ) {
    super();
  }

  ngOnInit() {
    this.prometheusService.ifAlertmanagerConfigured(() => {
      this.isAlertmanagerConfigured = true;
    });
    this.prometheusService.ifPrometheusConfigured(() => {
      this.isPrometheusConfigured = true;
    });
    this.summaryService.subscribeOnce((summary) => {
      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl = `https://docs.ceph.com/docs/${releaseName}/mgr/dashboard/#enabling-prometheus-alerting`;
    });
  }
}
