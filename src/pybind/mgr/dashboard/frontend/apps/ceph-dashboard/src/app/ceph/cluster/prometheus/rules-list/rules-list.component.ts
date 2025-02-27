import { Component, Inject, OnInit } from '@angular/core';

import _ from 'lodash';

import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { PrometheusListHelper } from '~/app/shared/helpers/prometheus-list-helper';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { PrometheusRule } from '~/app/shared/models/prometheus-alerts';
import { DurationPipe } from '~/app/shared/pipes/duration.pipe';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';

@Component({
  selector: 'cd-rules-list',
  templateUrl: './rules-list.component.html',
  styleUrls: ['./rules-list.component.scss']
})
export class RulesListComponent extends PrometheusListHelper implements OnInit {
  columns: CdTableColumn[];
  declare expandedRow: PrometheusRule;
  selection = new CdTableSelection();

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
      { prop: 'name', name: $localize`Name`, cellClass: 'fw-bold', flexGrow: 2 },
      {
        prop: 'labels.severity',
        name: $localize`Severity`,
        flexGrow: 1,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            critical: { class: 'badge-danger' },
            warning: { class: 'badge-warning' }
          }
        }
      },
      {
        prop: 'group',
        name: $localize`Group`,
        flexGrow: 1,
        cellTransformation: CellTemplate.badge
      },
      { prop: 'duration', name: $localize`Duration`, pipe: new DurationPipe(), flexGrow: 1 },
      { prop: 'query', name: $localize`Query`, isHidden: true, flexGrow: 1 },
      { prop: 'annotations.summary', name: $localize`Summary`, flexGrow: 3 }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
