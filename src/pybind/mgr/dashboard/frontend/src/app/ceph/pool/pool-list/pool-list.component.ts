import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';

import { ConfigurationService } from '../../../shared/api/configuration.service';
import { PoolService } from '../../../shared/api/pool.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '../../../shared/constants/app.constants';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { Icons } from '../../../shared/enum/icons.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permissions } from '../../../shared/models/permissions';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ModalService } from '../../../shared/services/modal.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { URLBuilderService } from '../../../shared/services/url-builder.service';
import { PgCategoryService } from '../../shared/pg-category.service';
import { Pool } from '../pool';
import { PoolStat, PoolStats } from '../pool-stat';

const BASE_URL = 'pool';

@Component({
  selector: 'cd-pool-list',
  templateUrl: './pool-list.component.html',
  providers: [
    TaskListService,
    { provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }
  ],
  styleUrls: ['./pool-list.component.scss']
})
export class PoolListComponent extends ListWithDetails implements OnInit {
  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('poolUsageTpl', { static: true })
  poolUsageTpl: TemplateRef<any>;

  @ViewChild('poolConfigurationSourceTpl')
  poolConfigurationSourceTpl: TemplateRef<any>;

  pools: Pool[];
  columns: CdTableColumn[];
  selection = new CdTableSelection();
  modalRef: NgbModalRef;
  executingTasks: ExecutingTask[] = [];
  permissions: Permissions;
  tableActions: CdTableAction[];
  viewCacheStatusList: any[];
  cacheTiers: any[] = [];
  monAllowPoolDelete = false;

  constructor(
    private poolService: PoolService,
    private taskWrapper: TaskWrapperService,
    private authStorageService: AuthStorageService,
    private taskListService: TaskListService,
    private modalService: ModalService,
    private pgCategoryService: PgCategoryService,
    private dimlessPipe: DimlessPipe,
    private urlBuilder: URLBuilderService,
    private configurationService: ConfigurationService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        routerLink: () => this.urlBuilder.getCreate(),
        name: this.actionLabels.CREATE
      },
      {
        permission: 'update',
        icon: Icons.edit,
        routerLink: () =>
          this.urlBuilder.getEdit(encodeURIComponent(this.selection.first().pool_name)),
        name: this.actionLabels.EDIT
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deletePoolModal(),
        name: this.actionLabels.DELETE,
        disable: () => !this.selection.first() || !this.monAllowPoolDelete,
        disableDesc: () => this.getDisableDesc()
      }
    ];

    // Note, we need read permissions to get the 'mon_allow_pool_delete'
    // configuration option.
    if (this.permissions.configOpt.read) {
      this.configurationService.get('mon_allow_pool_delete').subscribe((data: any) => {
        if (_.has(data, 'value')) {
          const monSection = _.find(data.value, (v) => {
            return v.section === 'mon';
          }) || { value: false };
          this.monAllowPoolDelete = monSection.value === 'true' ? true : false;
        }
      });
    }
  }

  ngOnInit() {
    const compare = (prop: string, pool1: Pool, pool2: Pool) =>
      _.get(pool1, prop) > _.get(pool2, prop) ? 1 : -1;
    this.columns = [
      {
        prop: 'pool_name',
        name: $localize`Name`,
        flexGrow: 4,
        cellTransformation: CellTemplate.executing
      },
      {
        prop: 'type',
        name: $localize`Type`,
        flexGrow: 2
      },
      {
        prop: 'application_metadata',
        name: $localize`Applications`,
        flexGrow: 3
      },
      {
        prop: 'pg_status',
        name: $localize`PG Status`,
        flexGrow: 3,
        cellClass: ({ row, column, value }): any => {
          return this.getPgStatusCellClass(row, column, value);
        }
      },
      {
        prop: 'size',
        name: $localize`Replica Size`,
        flexGrow: 2,
        cellClass: 'text-right'
      },
      {
        prop: 'last_change',
        name: $localize`Last Change`,
        flexGrow: 2,
        cellClass: 'text-right'
      },
      {
        prop: 'erasure_code_profile',
        name: $localize`Erasure Coded Profile`,
        flexGrow: 2
      },
      {
        prop: 'crush_rule',
        name: $localize`Crush Ruleset`,
        flexGrow: 3
      },
      {
        name: $localize`Usage`,
        prop: 'usage',
        cellTemplate: this.poolUsageTpl,
        flexGrow: 3
      },
      {
        prop: 'stats.rd_bytes.rates',
        name: $localize`Read bytes`,
        comparator: (_valueA: any, _valueB: any, rowA: Pool, rowB: Pool) =>
          compare('stats.rd_bytes.latest', rowA, rowB),
        cellTransformation: CellTemplate.sparkline,
        flexGrow: 3
      },
      {
        prop: 'stats.wr_bytes.rates',
        name: $localize`Write bytes`,
        comparator: (_valueA: any, _valueB: any, rowA: Pool, rowB: Pool) =>
          compare('stats.wr_bytes.latest', rowA, rowB),
        cellTransformation: CellTemplate.sparkline,
        flexGrow: 3
      },
      {
        prop: 'stats.rd.rate',
        name: $localize`Read ops`,
        flexGrow: 1,
        pipe: this.dimlessPipe,
        cellTransformation: CellTemplate.perSecond
      },
      {
        prop: 'stats.wr.rate',
        name: $localize`Write ops`,
        flexGrow: 1,
        pipe: this.dimlessPipe,
        cellTransformation: CellTemplate.perSecond
      }
    ];

    this.taskListService.init(
      () => this.poolService.getList(),
      undefined,
      (pools) => (this.pools = this.transformPoolsData(pools)),
      () => {
        this.table.reset(); // Disable loading indicator.
        this.viewCacheStatusList = [{ status: ViewCacheStatus.ValueException }];
      },
      (task) => task.name.startsWith(`${BASE_URL}/`),
      (pool, task) => task.metadata['pool_name'] === pool.pool_name,
      { default: (metadata: any) => new Pool(metadata['pool_name']) }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deletePoolModal() {
    const name = this.selection.first().pool_name;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'Pool',
      itemNames: [name],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask(`${BASE_URL}/${URLVerbs.DELETE}`, { pool_name: name }),
          call: this.poolService.delete(name)
        })
    });
  }

  getPgStatusCellClass(_row: any, _column: any, value: string): object {
    return {
      'text-right': true,
      [`pg-${this.pgCategoryService.getTypeByStates(value)}`]: true
    };
  }

  transformPoolsData(pools: any) {
    const requiredStats = [
      'bytes_used',
      'max_avail',
      'avail_raw',
      'percent_used',
      'rd_bytes',
      'wr_bytes',
      'rd',
      'wr'
    ];
    const emptyStat: PoolStat = { latest: 0, rate: 0, rates: [] };

    _.forEach(pools, (pool: Pool) => {
      pool['pg_status'] = this.transformPgStatus(pool['pg_status']);
      const stats: PoolStats = {};
      _.forEach(requiredStats, (stat) => {
        stats[stat] = pool.stats && pool.stats[stat] ? pool.stats[stat] : emptyStat;
      });
      pool['stats'] = stats;
      pool['usage'] = stats.percent_used.latest;

      if (
        !pool.cdExecuting &&
        pool.pg_num + pool.pg_placement_num !== pool.pg_num_target + pool.pg_placement_num_target
      ) {
        pool['cdExecuting'] = 'Updating';
      }

      ['rd_bytes', 'wr_bytes'].forEach((stat) => {
        pool.stats[stat].rates = pool.stats[stat].rates.map((point: any) => point[1]);
      });
      pool.cdIsBinary = true;
    });

    return pools;
  }

  transformPgStatus(pgStatus: any): string {
    const strings: string[] = [];
    _.forEach(pgStatus, (count, state) => {
      strings.push(`${count} ${state}`);
    });

    return strings.join(', ');
  }

  getSelectionTiers() {
    if (typeof this.expandedRow !== 'undefined') {
      const cacheTierIds = this.expandedRow['tiers'];
      this.cacheTiers = this.pools.filter((pool) => cacheTierIds.includes(pool.pool));
    }
  }

  getDisableDesc(): string | undefined {
    if (!this.monAllowPoolDelete) {
      return $localize`Pool deletion is disabled by the mon_allow_pool_delete configuration setting.`;
    }

    return undefined;
  }

  setExpandedRow(expandedRow: any) {
    super.setExpandedRow(expandedRow);
    this.getSelectionTiers();
  }
}
