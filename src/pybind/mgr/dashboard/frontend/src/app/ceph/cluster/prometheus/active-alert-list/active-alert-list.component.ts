import { Component, Inject, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { PrometheusListHelper } from '~/app/shared/helpers/prometheus-list-helper';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AlertState } from '~/app/shared/models/prometheus-alerts';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';

const BASE_URL = 'silences'; // as only silence actions can be used

@Component({
  selector: 'cd-active-alert-list',
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }],
  templateUrl: './active-alert-list.component.html',
  styleUrls: ['./active-alert-list.component.scss']
})
export class ActiveAlertListComponent extends PrometheusListHelper implements OnInit {
  @ViewChild('externalLinkTpl', { static: true })
  externalLinkTpl: TemplateRef<any>;
  columns: CdTableColumn[];
  innerColumns: CdTableColumn[];
  tableActions: CdTableAction[];
  permission: Permission;
  selection = new CdTableSelection();
  icons = Icons;
  expandedInnerRow: any;
  multilineTextKeys = ['description', 'impact', 'fix'];

  filters: CdTableColumn[] = [
    {
      name: $localize`State`,
      prop: 'status.state',
      filterOptions: [$localize`All`, $localize`Active`, $localize`Suppressed`],
      filterInitValue: $localize`Active`,
      filterPredicate: (row, value) => {
        if (value === 'Active') return row.status?.state === AlertState.ACTIVE;
        else if (value === 'Suppressed') return row.status?.state === AlertState.SUPPRESSED;
        if (value === 'All') return true;
        return false;
      }
    }
  ];

  constructor(
    // NotificationsComponent will refresh all alerts every 5s (No need to do it here as well)
    private authStorageService: AuthStorageService,
    public prometheusAlertService: PrometheusAlertService,
    private urlBuilder: URLBuilderService,
    @Inject(PrometheusService) prometheusService: PrometheusService
  ) {
    super(prometheusService);
    this.permission = this.authStorageService.getPermissions().prometheus;
    this.tableActions = [
      {
        permission: 'create',
        canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection || selection.first().cdExecuting,
        icon: Icons.add,
        routerLink: () =>
          '/monitoring' + this.urlBuilder.getCreateFrom(this.selection.first().fingerprint),
        name: $localize`Create Silence`
      }
    ];
  }

  ngOnInit() {
    super.ngOnInit();
    this.innerColumns = [
      {
        name: $localize`Description`,
        prop: 'annotations.description',
        flexGrow: 3
      },
      {
        name: $localize`Severity`,
        prop: 'labels.severity',
        flexGrow: 1,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          map: {
            critical: { class: 'tag-danger' },
            warning: { class: 'tag-warning' }
          }
        }
      },
      {
        name: $localize`State`,
        prop: 'status.state',
        flexGrow: 1,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          map: {
            active: { class: 'tag-info' },
            unprocessed: { class: 'tag-warning' },
            suppressed: { class: 'tag-dark' }
          }
        }
      },
      {
        name: $localize`Started`,
        prop: 'startsAt',
        cellTransformation: CellTemplate.timeAgo,
        flexGrow: 1
      }
    ];
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'labels.alertname',
        cellClass: 'fw-bold',
        flexGrow: 2
      },
      {
        name: $localize`Summary`,
        prop: 'annotations.summary',
        flexGrow: 3
      },
      ...this.innerColumns.slice(1),
      {
        name: $localize`Occurrence`,
        prop: 'alert_count',
        flexGrow: 1
      },
      {
        name: $localize`URL`,
        prop: 'generatorURL',
        flexGrow: 1,
        sortable: false,
        cellTemplate: this.externalLinkTpl
      }
    ];
    this.prometheusAlertService.getGroupedAlerts(true);
  }

  setExpandedInnerRow(row: any) {
    this.expandedInnerRow = row;
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
