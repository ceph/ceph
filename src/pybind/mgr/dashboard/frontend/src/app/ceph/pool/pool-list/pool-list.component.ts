import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';

import { PoolService } from '../../../shared/api/pool.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permissions } from '../../../shared/models/permissions';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { PgCategoryService } from '../../shared/pg-category.service';
import { Pool } from '../pool';

@Component({
  selector: 'cd-pool-list',
  templateUrl: './pool-list.component.html',
  providers: [TaskListService],
  styleUrls: ['./pool-list.component.scss']
})
export class PoolListComponent implements OnInit {
  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('poolUsageTpl')
  poolUsageTpl: TemplateRef<any>;

  pools: Pool[] = [];
  columns: CdTableColumn[];
  selection = new CdTableSelection();
  modalRef: BsModalRef;
  executingTasks: ExecutingTask[] = [];
  permissions: Permissions;
  tableActions: CdTableAction[];
  viewCacheStatusList: any[];
  selectionCacheTiers: any[] = [];

  constructor(
    private poolService: PoolService,
    private taskWrapper: TaskWrapperService,
    private authStorageService: AuthStorageService,
    private taskListService: TaskListService,
    private modalService: BsModalService,
    private i18n: I18n,
    private pgCategoryService: PgCategoryService,
    private dimlessPipe: DimlessPipe
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.tableActions = [
      {
        permission: 'create',
        icon: 'fa-plus',
        routerLink: () => '/pool/add',
        name: this.i18n('Add')
      },
      {
        permission: 'update',
        icon: 'fa-pencil',
        routerLink: () => '/pool/edit/' + this.selection.first().pool_name,
        name: this.i18n('Edit')
      },
      {
        permission: 'delete',
        icon: 'fa-trash-o',
        click: () => this.deletePoolModal(),
        name: this.i18n('Delete')
      }
    ];
  }

  ngOnInit() {
    this.columns = [
      {
        prop: 'pool_name',
        name: this.i18n('Name'),
        flexGrow: 4,
        cellTransformation: CellTemplate.executing
      },
      {
        prop: 'type',
        name: this.i18n('Type'),
        flexGrow: 2
      },
      {
        prop: 'application_metadata',
        name: this.i18n('Applications'),
        flexGrow: 2
      },
      {
        prop: 'pg_status',
        name: this.i18n('PG Status'),
        flexGrow: 3,
        cellClass: ({ row, column, value }): any => {
          return this.getPgStatusCellClass({ row, column, value });
        }
      },
      {
        prop: 'size',
        name: this.i18n('Replica Size'),
        flexGrow: 1,
        cellClass: 'text-right'
      },
      {
        prop: 'last_change',
        name: this.i18n('Last Change'),
        flexGrow: 1,
        cellClass: 'text-right'
      },
      {
        prop: 'erasure_code_profile',
        name: this.i18n('Erasure Coded Profile'),
        flexGrow: 2
      },
      {
        prop: 'crush_rule',
        name: this.i18n('Crush Ruleset'),
        flexGrow: 3
      },
      { name: this.i18n('Usage'), cellTemplate: this.poolUsageTpl, flexGrow: 3 },
      {
        prop: 'stats.rd_bytes.series',
        name: this.i18n('Read bytes'),
        cellTransformation: CellTemplate.sparkline,
        flexGrow: 3
      },
      {
        prop: 'stats.wr_bytes.series',
        name: this.i18n('Write bytes'),
        cellTransformation: CellTemplate.sparkline,
        flexGrow: 3
      },
      {
        prop: 'stats.rd.rate',
        name: this.i18n('Read ops'),
        flexGrow: 1,
        pipe: this.dimlessPipe,
        cellTransformation: CellTemplate.perSecond
      },
      {
        prop: 'stats.wr.rate',
        name: this.i18n('Write ops'),
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
      (task) => task.name.startsWith('pool/'),
      (pool, task) => task.metadata['pool_name'] === pool.pool_name,
      { default: (task: ExecutingTask) => new Pool(task.metadata['pool_name']) }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
    this.getSelectionTiers();
  }

  deletePoolModal() {
    const name = this.selection.first().pool_name;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: 'Pool',
        submitActionObservable: () =>
          this.taskWrapper.wrapTaskAroundCall({
            task: new FinishedTask('pool/delete', { pool_name: name }),
            call: this.poolService.delete(name)
          })
      }
    });
  }

  getPgStatusCellClass({ row, column, value }): object {
    return {
      'text-right': true,
      [`pg-${this.pgCategoryService.getTypeByStates(value)}`]: true
    };
  }

  transformPoolsData(pools: any) {
    const requiredStats = ['bytes_used', 'max_avail', 'rd_bytes', 'wr_bytes', 'rd', 'wr'];
    const emptyStat = { latest: 0, rate: 0, series: [] };

    _.forEach(pools, (pool: Pool) => {
      pool['pg_status'] = this.transformPgStatus(pool['pg_status']);
      const stats = {};
      _.forEach(requiredStats, (stat) => {
        stats[stat] = pool.stats && pool.stats[stat] ? pool.stats[stat] : emptyStat;
      });
      pool['stats'] = stats;

      ['rd_bytes', 'wr_bytes'].forEach((stat) => {
        pool.stats[stat].series = pool.stats[stat].series.map((point) => point[1]);
      });
      pool.cdIsBinary = true;
    });

    return pools;
  }

  transformPgStatus(pgStatus: any): string {
    const strings = [];
    _.forEach(pgStatus, (count, state) => {
      strings.push(`${count} ${state}`);
    });

    return strings.join(', ');
  }

  getPoolDetails(pool: object) {
    return _.omit(pool, ['cdExecuting', 'cdIsBinary']);
  }

  getSelectionTiers() {
    const cacheTierIds = this.selection.hasSingleSelection ? this.selection.first()['tiers'] : [];
    this.selectionCacheTiers = this.pools.filter((pool) => cacheTierIds.includes(pool.pool));
  }
}
