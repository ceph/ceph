import { Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { TabDirective, TabsetComponent } from 'ngx-bootstrap/tabs';

import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { PrometheusAlertService } from '../../../../shared/services/prometheus-alert.service';

@Component({
  selector: 'cd-monitoring-list',
  templateUrl: './monitoring-list.component.html',
  styleUrls: ['./monitoring-list.component.scss']
})
export class MonitoringListComponent implements OnInit {
  constructor(
    public prometheusAlertService: PrometheusAlertService,
    private prometheusService: PrometheusService,
    private route: ActivatedRoute,
    private router: Router
  ) {}
  @ViewChild('tabs', { static: true })
  tabs: TabsetComponent;

  isPrometheusConfigured = false;
  isAlertmanagerConfigured = false;

  ngOnInit() {
    this.prometheusService.ifAlertmanagerConfigured(() => {
      this.isAlertmanagerConfigured = true;
    });
    this.prometheusService.ifPrometheusConfigured(() => {
      this.isPrometheusConfigured = true;
    });

    // Activate tab according to given fragment
    if (this.route.snapshot.fragment) {
      const tab = this.tabs.tabs.find(
        (t) => t.elementRef.nativeElement.id === this.route.snapshot.fragment
      );
      if (tab) {
        tab.active = true;
      }
      // Ensure fragment is not removed, so page can always be reloaded with the same tab open.
      this.router.navigate([], { fragment: this.route.snapshot.fragment });
    }
  }

  setFragment(element: TabDirective) {
    this.router.navigate([], { fragment: element.id });
  }
}
