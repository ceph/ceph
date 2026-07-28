import { Component, Inject, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Subscription } from 'rxjs';
import { filter, first } from 'rxjs/operators';

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
import { DocService } from '~/app/shared/services/doc.service';

const BASE_URL = 'silences'; // as only silence actions can be used

const SeverityMap = {
  critical: $localize`Critical`,
  warning: $localize`Warning`,
  all: $localize`All`
};

@Component({
  selector: 'cd-active-alert-list',
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }],
  templateUrl: './active-alert-list.component.html',
  styleUrls: ['./active-alert-list.component.scss'],
  standalone: false
})
export class ActiveAlertListComponent extends PrometheusListHelper implements OnInit, OnDestroy {
  @ViewChild('externalLinkTpl', { static: true })
  externalLinkTpl: TemplateRef<any>;
  @ViewChild('docLinkTpl', { static: true })
  docLinkTpl: TemplateRef<any>;
  columns: CdTableColumn[];
  innerColumns: CdTableColumn[];
  tableActions: CdTableAction[];
  permission: Permission;
  selection = new CdTableSelection();
  icons = Icons;
  expandedInnerRow: any;
  alertDocUrls: Record<string, string | null> = {};
  hasDocUrls = false;
  multilineTextKeys = ['description', 'impact', 'fix'];
  private cephRelease = '';
  private alertsSub: Subscription;

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
    },
    {
      name: $localize`Severity`,
      prop: 'labels.severity',
      filterOptions: [SeverityMap['all'], SeverityMap['warning'], SeverityMap['critical']],
      filterInitValue: SeverityMap['all'],
      filterPredicate: (row, value) => {
        if (value === SeverityMap['critical']) return row.labels?.severity === 'critical';
        else if (value === SeverityMap['warning']) return row.labels?.severity === 'warning';
        if (value === SeverityMap['all']) return true;
        return false;
      }
    }
  ];

  constructor(
    // NotificationsComponent will refresh all alerts every 5s (No need to do it here as well)
    private authStorageService: AuthStorageService,
    public prometheusAlertService: PrometheusAlertService,
    private urlBuilder: URLBuilderService,
    private route: ActivatedRoute,
    private docService: DocService,
    @Inject(PrometheusService) prometheusService: PrometheusService
  ) {
    super(prometheusService);
    this.permission = this.authStorageService.getPermissions().prometheus;
    this.docService.releaseData$
      .pipe(
        filter((v) => !!v),
        first()
      )
      .subscribe((release) => {
        this.cephRelease = release;
        this.hasDocUrls = !!this.docService.urlGenerator('managing-alerts', release);
        this.alertDocUrls = {};
      });
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
        name: $localize`Query`,
        prop: 'generatorURL',
        flexGrow: 1,
        sortable: false,
        cellTemplate: this.externalLinkTpl
      },
      ...(this.hasDocUrls
        ? [
            {
              name: $localize`Learn more`,
              prop: 'labels.alertname',
              flexGrow: 1,
              sortable: false,
              cellTemplate: this.docLinkTpl
            }
          ]
        : [])
    ];
    this.alertsSub = this.prometheusAlertService.totalAlerts$.subscribe(() => {
      if (!this.hasDocUrls) return;
      this.alertDocUrls = Object.fromEntries(
        this.prometheusAlertService.alerts.map((a) => [
          a.labels.alertname,
          this.docService.alertDocUrl(a.labels.alertname, this.cephRelease)
        ])
      );
    });
    this.prometheusAlertService.getGroupedAlerts(true);
    this.route.queryParams.subscribe((params) => {
      const severity = params['severity'];
      this.filters[1].filterInitValue = SeverityMap[severity];
    });
  }

  ngOnDestroy() {
    this.alertsSub?.unsubscribe();
  }

  setExpandedInnerRow(row: any) {
    this.expandedInnerRow = row;
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
