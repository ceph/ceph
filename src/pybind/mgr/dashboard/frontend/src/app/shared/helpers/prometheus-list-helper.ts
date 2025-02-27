import { Directive, OnInit } from '@angular/core';

import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';

@Directive()
// tslint:disable-next-line: directive-class-suffix
export class PrometheusListHelper extends ListWithDetails implements OnInit {
  public isPrometheusConfigured = false;
  public isAlertmanagerConfigured = false;

  constructor(protected prometheusService: PrometheusService) {
    super();
  }

  ngOnInit() {
    this.prometheusService.ifAlertmanagerConfigured(() => {
      this.isAlertmanagerConfigured = true;
    });
    this.prometheusService.ifPrometheusConfigured(() => {
      this.isPrometheusConfigured = true;
    });
  }
}
