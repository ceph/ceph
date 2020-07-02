import { Component, Inject, OnInit } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { PrometheusRule } from '../../../../shared/models/prometheus-alerts';
import { CephReleaseNamePipe } from '../../../../shared/pipes/ceph-release-name.pipe';
import { DurationPipe } from '../../../../shared/pipes/duration.pipe';
import { PrometheusAlertService } from '../../../../shared/services/prometheus-alert.service';
import { SummaryService } from '../../../../shared/services/summary.service';
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
    private i18n: I18n,
    public prometheusAlertService: PrometheusAlertService,
    @Inject(PrometheusService) prometheusService: PrometheusService,
    @Inject(SummaryService) summaryService: SummaryService,
    @Inject(CephReleaseNamePipe) cephReleaseNamePipe: CephReleaseNamePipe
  ) {
    super(prometheusService, summaryService, cephReleaseNamePipe);
  }

  ngOnInit() {
    super.ngOnInit();
    this.columns = [
      { prop: 'name', name: this.i18n('Name') },
      { prop: 'labels.severity', name: this.i18n('Severity') },
      { prop: 'group', name: this.i18n('Group') },
      { prop: 'duration', name: this.i18n('Duration'), pipe: new DurationPipe() },
      { prop: 'query', name: this.i18n('Query'), isHidden: true },
      { prop: 'annotations.description', name: this.i18n('Description') }
    ];
  }
}
