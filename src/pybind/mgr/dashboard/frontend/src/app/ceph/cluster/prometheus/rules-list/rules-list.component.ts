import { Component, Inject, OnInit } from '@angular/core';

import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { PrometheusRule } from '~/app/shared/models/prometheus-alerts';
import { DurationPipe } from '~/app/shared/pipes/duration.pipe';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { PrometheusListHelper } from '../prometheus-list-helper';

@Component({
  selector: 'cd-rules-list',
  templateUrl: './rules-list.component.html',
  styleUrls: ['./rules-list.component.scss']
})
export class RulesListComponent extends PrometheusListHelper implements OnInit {
  columns: CdTableColumn[];
  expandedRow: PrometheusRule;

  /**
   * Hide active alerts in details of alerting rules as they are already shown
   * in the 'active alerts' table. Also hide the 'type' column as the type is
   * always supposed to be 'alerting'.
   */
  hideKeys = ['alerts', 'type'];

  constructor(
    public prometheusAlertService: PrometheusAlertService,
    @Inject(PrometheusService) prometheusService: PrometheusService
  ) {
    super(prometheusService);
  }

  ngOnInit() {
    super.ngOnInit();
    this.columns = [
      { prop: 'name', name: $localize`Name` },
      { prop: 'labels.severity', name: $localize`Severity` },
      { prop: 'group', name: $localize`Group` },
      { prop: 'duration', name: $localize`Duration`, pipe: new DurationPipe() },
      { prop: 'query', name: $localize`Query`, isHidden: true },
      { prop: 'annotations.description', name: $localize`Description` }
    ];
  }
}
