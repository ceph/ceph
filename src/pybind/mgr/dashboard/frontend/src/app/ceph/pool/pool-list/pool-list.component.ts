import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import _ from 'lodash';
import { BehaviorSubject, forkJoin } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { PgCategoryService } from '~/app/ceph/shared/pg-category.service';
import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { ErasureCodeProfileService } from '~/app/shared/api/erasure-code-profile.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { TableStatusViewCache } from '~/app/shared/classes/table-status-view-cache';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeleteGuardModalComponent } from '~/app/shared/components/delete-guard-modal/delete-guard-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ViewCacheStatus } from '~/app/shared/enum/view-cache-status.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ErasureCodeProfile } from '~/app/shared/models/erasure-code-profile';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { isReleaseAtLeast, mapPoolApplications, Pool, PoolType, transformPgStatus } from '../pool';
import { PoolStat, PoolStats } from '../pool-stat';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Router } from '@angular/router';

const EXPERIMENTAL_CONFIG = 'enable_experimental_unrecoverable_data_corrupting_features';

const BASE_URL = 'pool';
const POOL_CONFIG = 'mon_allow_pool_delete';

interface PoolTaskMetadata {
  pool_name: string;
}

@Component({
  selector: 'cd-pool-list',
  templateUrl: './pool-list.component.html',
  providers: [
    TaskListService,
    { provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }
  ],
  styleUrls: ['./pool-list.component.scss'],
  standalone: false
})
export class PoolListComponent extends ListWithDetails implements OnInit {
  @ViewChild(TableComponent)
  table!: TableComponent;
  @ViewChild('poolNameTpl', { static: true })
  poolNameTpl!: TemplateRef<any>;

  @ViewChild('poolConfigurationSourceTpl')
  poolConfigurationSourceTpl!: TemplateRef<any>;

  @ViewChild('poolEnableTpl', { static: true })
  poolEnableTpl!: TemplateRef<any>;

  @ViewChild('poolNoPermsTpl', { static: true })
  poolNoPermsTpl!: TemplateRef<any>;

  pools: Pool[] = [];
  columns: CdTableColumn[] = [];
  selection = new CdTableSelection();
  executingTasks: ExecutingTask[] = [];
  permissions: Permissions;
  tableActions: CdTableAction[] = [];
  tableStatus = new TableStatusViewCache();
  cacheTiers: any[] = [];
  monAllowPoolDelete$ = new BehaviorSubject<boolean>(false);
  ecProfileList: ErasureCodeProfile[] = [];
  viewUrl = '/pool/view';
  migrationPreconditions = {
    experimentalFlag: false,
    minCompatClient: false,
    osdRelease: false
  };

  constructor(
    private poolService: PoolService,
    private taskWrapper: TaskWrapperService,
    private ecpService: ErasureCodeProfileService,
    private authStorageService: AuthStorageService,
    public taskListService: TaskListService,
    private modalService: ModalCdsService,
    private pgCategoryService: PgCategoryService,
    private urlBuilder: URLBuilderService,
    private configurationService: ConfigurationService,
    public actionLabels: ActionLabelsI18n,
    private router: Router
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
        name: this.actionLabels.EDIT,
        disable: (s: CdTableSelection) => this.getDisabledExecutingReason(s)
      },
      {
        permission: 'create',
        icon: Icons.exchange,
        click: () => this.migratePool(),
        name: this.actionLabels.MIGRATE,
        disable: (s: CdTableSelection) => this.getDisabledMigrateReason(s)
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deletePoolModal(),
        name: this.actionLabels.DELETE,
        disable: (s: CdTableSelection) => this.getDisabledExecutingReason(s)
      }
    ];

    // Note, we need read permissions to get the 'mon_allow_pool_delete'
    // configuration option.
    if (this.permissions.configOpt.read) {
      this.configurationService.get(POOL_CONFIG).subscribe((data: any) => {
        if (_.has(data, 'value')) {
          const monSection = _.find(data.value, (v) => {
            return v.section === 'mon';
          }) || { value: false };
          this.monAllowPoolDelete$.next(monSection.value === 'true');
        }
      });
    }
  }

  ngOnInit() {
    this.checkMigrationPreconditions();

    this.columns = [
      {
        prop: 'pool_name',
        name: $localize`Name`,
        flexGrow: 2,
        cellTemplate: this.poolNameTpl
      },
      {
        prop: 'data_protection',
        name: $localize`Data Protection`,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          class: 'tag-background-gray'
        },
        flexGrow: 1.3
      },
      {
        prop: 'application_metadata',
        name: $localize`Applications`,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          class: 'tag-background-primary'
        },
        flexGrow: 1.5
      },
      {
        prop: 'pg_status',
        name: $localize`PG Status`,
        flexGrow: 1.2,
        cellClass: ({ row, column, value }): any => {
          return this.getPgStatusCellClass(row, column, value);
        }
      }
    ];

    this.taskListService.init(
      () =>
        this.ecpService.list().pipe(
          mergeMap((ecProfileList: ErasureCodeProfile[]) => {
            this.ecProfileList = ecProfileList;
            return this.poolService.getList();
          })
        ),
      (resp: Pool[]) => resp,
      (pools: Pool[]) => {
        this.pools = this.transformPoolsData(pools);
        this.tableStatus = new TableStatusViewCache();
      },
      () => {
        this.table.reset(); // Disable loading indicator.
        this.tableStatus = new TableStatusViewCache(ViewCacheStatus.ValueException);
      },
      (task) => task.name.startsWith(`${BASE_URL}/`),
      (pool: Pool, task) => (task.metadata as PoolTaskMetadata).pool_name === pool.pool_name,
      { default: (metadata: PoolTaskMetadata) => new Pool(metadata.pool_name) }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deletePoolModal() {
    const name = this.selection.first().pool_name;
    if (!this.monAllowPoolDelete$.getValue()) {
      if (this.permissions.configOpt.read && this.permissions.pool.read) {
        this.modalService.show(ConfirmationModalComponent, {
          titleText: $localize`Can't delete ${name}`,
          buttonText: $localize`Enable`,
          bodyTpl: this.poolEnableTpl,
          warning: true,
          onSubmit: () => this.enablePoolDeletion()
        });
      } else {
        this.modalService.show(DeleteGuardModalComponent, {
          resourceName: name,
          resourceType: $localize`Pool`,
          bodyTemplate: this.poolNoPermsTpl
        });
      }
      return;
    }
    this.modalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.high,
      itemDescription: $localize`Pool`,
      itemNames: [name],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask(`${BASE_URL}/${URLVerbs.DELETE}`, { pool_name: name }),
          call: this.poolService.delete(name)
        })
    });
  }

  migratePool() {
    const poolName = this.selection.first().pool_name;
    this.router.navigate([`/pool/${URLVerbs.MIGRATE}`, encodeURIComponent(poolName)]);
  }

  enablePoolDeletion() {
    this.configurationService
      .create({
        name: POOL_CONFIG,
        value: [{ section: 'mon', value: true }],
        force_update: false
      })
      .subscribe(() => {
        this.monAllowPoolDelete$.next(true);
        this.modalService.dismissAll();
      });
  }

  getPgStatusCellClass(_row: any, _column: any, value: string): object {
    return {
      'text-right': true,
      [`pg-${this.pgCategoryService.getTypeByStates(value)}`]: true
    };
  }

  getErasureCodeProfile(erasureCodeProfile: string) {
    let ecpInfo = '';
    _.forEach(this.ecProfileList, (ecpKey) => {
      if (ecpKey['name'] === erasureCodeProfile) {
        ecpInfo = `EC: ${ecpKey['k']}+${ecpKey['m']}`;
      }
    });
    return ecpInfo;
  }

  transformPoolsData(pools: Pool[]): Pool[] {
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
      pool['pg_status'] = transformPgStatus(pool['pg_status']);
      const stats: PoolStats = {};
      _.forEach(requiredStats, (stat) => {
        stats[stat] = pool.stats?.[stat] ? pool.stats[stat] : emptyStat;
      });
      pool['stats'] = stats;
      pool['usage'] = stats.percent_used?.latest ?? 0;

      if (!pool.cdExecuting) {
        if (pool.migration_src != null || pool.migration_target != null) {
          pool['cdExecuting'] = 'Migrating';
        } else if (
          pool.pg_num + pool.pg_placement_num !==
          pool.pg_num_target + pool.pg_placement_num_target
        ) {
          pool['cdExecuting'] = 'Updating';
        }
      }

      pool['migrationTag'] = this.getMigrationTag(pool);

      ['rd_bytes', 'wr_bytes'].forEach((stat) => {
        const statData = pool.stats?.[stat] || emptyStat;
        statData.rates = statData.rates.map((point: any) => point[1]);
        const poolStats = pool.stats || (pool.stats = {} as PoolStats);
        poolStats[stat] = statData;
      });
      pool.cdIsBinary = true;

      if (pool['type'] === PoolType.ERASURE) {
        const erasureCodeProfile = pool['erasure_code_profile'];
        pool['data_protection'] = this.getErasureCodeProfile(erasureCodeProfile);
      }
      if (pool['type'] === PoolType.REPLICATED) {
        pool['data_protection'] = `replica: ×${pool['size']}`;
      }

      pool['application_metadata'] = mapPoolApplications(pool.application_metadata || []);
    });

    return pools;
  }

  getSelectionTiers() {
    if (typeof this.expandedRow !== 'undefined') {
      const cacheTierIds = this.expandedRow['tiers'];
      this.cacheTiers = this.pools.filter((pool) => cacheTierIds.includes(pool.pool));
    }
  }

  setExpandedRow(expandedRow: any) {
    super.setExpandedRow(expandedRow);
    this.getSelectionTiers();
  }

  private getDisabledExecutingReason(selection: CdTableSelection): boolean | string {
    if (!selection.hasSingleSelection) {
      return true;
    }
    const pool = selection.first();
    if (pool.cdExecuting === 'Migrating') {
      return $localize`Actions are disabled while pool migration is in progress`;
    }
    return pool.cdExecuting ? true : false;
  }

  private getDisabledMigrateReason(selection: CdTableSelection): boolean | string {
    const executingReason = this.getDisabledExecutingReason(selection);
    if (executingReason) {
      return executingReason;
    }

    const pool = selection.first();
    const reasons: string[] = [];

    if (!this.migrationPreconditions.experimentalFlag) {
      reasons.push($localize`Pool migration experimental flag is not enabled`);
    }
    if (!this.migrationPreconditions.minCompatClient) {
      reasons.push($localize`Minimum compatible client must be umbrella or later`);
    }
    if (!this.migrationPreconditions.osdRelease) {
      reasons.push($localize`OSD release must be umbrella or later`);
    }

    const pgStable =
      pool.pg_num === pool.pg_num_target && pool.pg_placement_num === pool.pg_placement_num_target;
    if (!pgStable) {
      reasons.push($localize`PG autoscaling is still in progress`);
    }

    if (reasons.length > 0) {
      return reasons.join('\n');
    }

    return false;
  }

  private checkMigrationPreconditions(): void {
    forkJoin([
      this.configurationService.get(EXPERIMENTAL_CONFIG),
      this.poolService.getInfo()
    ]).subscribe(([expConfig, info]: [any, any]) => {
      const globalVal = this.configurationService.getValue(expConfig, 'global');
      if (globalVal && typeof globalVal === 'string') {
        this.migrationPreconditions.experimentalFlag =
          globalVal === '*' ||
          globalVal
            .split(',')
            .map((s: string) => s.trim())
            .includes('poolmigration');
      }

      const requireMinCompatClient = info?.require_min_compat_client || '';
      this.migrationPreconditions.minCompatClient = isReleaseAtLeast(
        requireMinCompatClient,
        'umbrella'
      );

      const requireOsdRelease = info?.require_osd_release || '';
      this.migrationPreconditions.osdRelease = isReleaseAtLeast(requireOsdRelease, 'umbrella');
    });
  }

  private getMigrationTag(pool: Pool): { label: string; cssClass: string } | null {
    const hasMigrationError = pool['pg_status']
      ?.toString()
      .match(/migration_(toofull|unfound|error)/);

    if (pool.migration_src != null || pool.migration_target != null) {
      if (hasMigrationError) {
        return { label: $localize`Migration Error`, cssClass: 'tag-danger' };
      }
      return { label: $localize`Migrating`, cssClass: 'tag-info' };
    }

    if (pool.pool_name?.startsWith('.migrate-')) {
      return { label: $localize`Migration Stub`, cssClass: 'tag-background-gray' };
    }

    return null;
  }
}
