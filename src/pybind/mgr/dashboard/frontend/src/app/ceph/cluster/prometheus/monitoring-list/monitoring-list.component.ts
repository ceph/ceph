import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { CephReleaseNamePipe } from '../../../../shared/pipes/ceph-release-name.pipe';
import { PrometheusAlertService } from '../../../../shared/services/prometheus-alert.service';
import { SummaryService } from '../../../../shared/services/summary.service';

@Component({
  selector: 'cd-monitoring-list',
  templateUrl: './monitoring-list.component.html',
  styleUrls: ['./monitoring-list.component.scss']
})
export class MonitoringListComponent implements OnInit {
  constructor(
    public prometheusAlertService: PrometheusAlertService,
    private prometheusService: PrometheusService,
    public route: ActivatedRoute,
    private router: Router,
    private summaryService: SummaryService,
    private cephReleaseNamePipe: CephReleaseNamePipe
  ) {}
  isPrometheusConfigured = false;
  isAlertmanagerConfigured = false;

  docsUrl = '';

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

  setFragment(element: string) {
    this.router.navigate([], { fragment: element });
  }
}
