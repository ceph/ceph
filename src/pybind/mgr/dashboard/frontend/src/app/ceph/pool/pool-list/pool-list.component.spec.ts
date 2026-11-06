import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { of, throwError } from 'rxjs';

import { RbdConfigurationListComponent } from '~/app/ceph/block/rbd-configuration-list/rbd-configuration-list.component';
import { PgCategoryService } from '~/app/ceph/shared/pg-category.service';
import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { ErasureCodeProfileService } from '~/app/shared/api/erasure-code-profile.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ErasureCodeProfile } from '~/app/shared/models/erasure-code-profile';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, expectItemTasks, Mocks } from '~/testing/unit-test-helper';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import {
  addPoolMigrationFlag,
  getPoolBusyReason,
  isPoolMigrationFlagEnabled,
  isReleaseAtLeast,
  Pool,
  PoolAction,
  PoolBusyReason,
  transformPgStatus
} from '../pool';
import { PoolMigrationClusterSettingsModalComponent } from '../pool-migration-cluster-settings-modal/pool-migration-cluster-settings-modal.component';
import { PoolMigrationOsdReleaseModalComponent } from '../pool-migration-osd-release-modal/pool-migration-osd-release-modal.component';
import { PoolBusyModalComponent } from '../pool-busy-modal/pool-busy-modal.component';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { DeleteGuardModalComponent } from '~/app/shared/components/delete-guard-modal/delete-guard-modal.component';
import { PoolListComponent } from './pool-list.component';

describe('PoolListComponent', () => {
  let component: PoolListComponent;
  let fixture: ComponentFixture<PoolListComponent>;
  let poolService: PoolService;
  let getECPList: jasmine.Spy;

  const getPoolList = (): Pool[] => {
    return [Mocks.getPool('a', 0), Mocks.getPool('b', 1), Mocks.getPool('c', 2)];
  };
  const getECPProfiles = (): ErasureCodeProfile[] => {
    const ecpProfile = new ErasureCodeProfile();
    ecpProfile.name = 'default';
    ecpProfile.k = 2;
    ecpProfile.m = 1;

    return [ecpProfile];
  };

  configureTestBed({
    declarations: [PoolListComponent, RbdConfigurationListComponent],
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      RouterTestingModule,
      NgbNavModule,
      HttpClientTestingModule
    ],
    providers: [PgCategoryService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolListComponent);
    component = fixture.componentInstance;
    component.permissions.pool.read = true;
    poolService = TestBed.inject(PoolService);
    spyOn(poolService, 'getList').and.callFake(() => of(getPoolList()));
    getECPList = spyOn(TestBed.inject(ErasureCodeProfileService), 'list');
    getECPList.and.returnValue(of(getECPProfiles()));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have columns that are sortable', () => {
    expect(
      component.columns
        .filter((column) => !(column.prop === undefined))
        .every((column) => Boolean(column.prop))
    ).toBeTruthy();
  });

  describe('monAllowPoolDelete', () => {
    let configOptRead: boolean;
    let configurationService: ConfigurationService;

    beforeEach(() => {
      configOptRead = true;
      spyOn(TestBed.inject(AuthStorageService), 'getPermissions').and.callFake(() => ({
        configOpt: { read: configOptRead },
        pool: { read: true }
      }));
      configurationService = TestBed.inject(ConfigurationService);
    });

    it('should set value correctly if mon_allow_pool_delete flag is set to true', () => {
      const configOption = {
        name: 'mon_allow_pool_delete',
        value: [
          {
            section: 'mon',
            value: 'true'
          }
        ]
      };
      spyOn(configurationService, 'get').and.returnValue(of(configOption));
      fixture = TestBed.createComponent(PoolListComponent);
      component = fixture.componentInstance;
      expect(component.monAllowPoolDelete$.value).toBe(true);
    });

    it('should set value correctly if mon_allow_pool_delete flag is set to false', () => {
      const configOption = {
        name: 'mon_allow_pool_delete',
        value: [
          {
            section: 'mon',
            value: 'false'
          }
        ]
      };
      spyOn(configurationService, 'get').and.returnValue(of(configOption));
      fixture = TestBed.createComponent(PoolListComponent);
      component = fixture.componentInstance;
      expect(component.monAllowPoolDelete$.value).toBe(false);
    });

    it('should set value correctly if mon_allow_pool_delete flag is not set', () => {
      const configOption = {
        name: 'mon_allow_pool_delete'
      };
      spyOn(configurationService, 'get').and.returnValue(of(configOption));
      fixture = TestBed.createComponent(PoolListComponent);
      component = fixture.componentInstance;
      expect(component.monAllowPoolDelete$.value).toBe(false);
    });

    it('should set value to false w/o config-opt read privileges', () => {
      configOptRead = false;
      fixture = TestBed.createComponent(PoolListComponent);
      component = fixture.componentInstance;
      expect(component.monAllowPoolDelete$.value).toBe(false);
    });
  });

  describe('pool deletion', () => {
    let taskWrapper: TaskWrapperService;
    let modalRef: any;

    const setSelectedPool = (poolName: string) =>
      (component.selection.selected = [{ pool_name: poolName }]);

    const callDeletion = () => {
      component.deletePoolModal();
      expect(modalRef).toBeTruthy();
      const deletion: DeleteConfirmationModalComponent = modalRef && modalRef.componentInstance;
      deletion.submitActionObservable();
    };

    const testPoolDeletion = (poolName: string) => {
      setSelectedPool(poolName);
      callDeletion();
      expect(poolService.delete).toHaveBeenCalledWith(poolName);
      expect(taskWrapper.wrapTaskAroundCall).toHaveBeenCalledWith({
        task: {
          name: 'pool/delete',
          metadata: {
            pool_name: poolName
          }
        },
        call: undefined // because of stub
      });
    };

    beforeEach(() => {
      spyOn(TestBed.inject(ModalService), 'show').and.callFake((deletionClass, initialState) => {
        modalRef = {
          componentInstance: Object.assign(new deletionClass(), initialState)
        };
        return modalRef;
      });
      spyOn(poolService, 'delete').and.stub();
      taskWrapper = TestBed.inject(TaskWrapperService);
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
    });

    // @TODO: skipping this for now, as the e2e is covering this already
    // We'll need to fix it once the carbon works are done.
    it.skip('should pool deletion with two different pools', () => {
      testPoolDeletion('somePoolName');
      testPoolDeletion('aDifferentPoolName');
    });
  });

  describe('handling of executing tasks', () => {
    let summaryService: SummaryService;

    const addTask = (name: string, pool: string) => {
      const task = new ExecutingTask();
      task.name = name;
      task.metadata = { pool_name: pool };
      summaryService.addRunningTask(task);
    };

    beforeEach(() => {
      summaryService = TestBed.inject(SummaryService);
      summaryService['summaryDataSource'].next({
        executing_tasks: [],
        finished_tasks: []
      });
    });

    it('gets all pools without executing pools', () => {
      expect(component.pools.length).toBe(3);
      expect(component.pools.every((pool) => !pool.executingTasks)).toBeTruthy();
    });

    it('gets a pool from a task during creation', () => {
      addTask('pool/create', 'd');
      expect(component.pools.length).toBe(4);
      expectItemTasks(component.pools[3], 'Creating');
    });

    it('gets all pools with one executing pools', () => {
      addTask('pool/create', 'a');
      expect(component.pools.length).toBe(3);
      expectItemTasks(component.pools[0], 'Creating');
      expect(component.pools[1].cdExecuting).toBeFalsy();
      expect(component.pools[2].cdExecuting).toBeFalsy();
    });

    it('gets all pools with multiple executing pools', () => {
      addTask('pool/create', 'a');
      addTask('pool/edit', 'a');
      addTask('pool/delete', 'a');
      addTask('pool/edit', 'b');
      addTask('pool/delete', 'b');
      addTask('pool/delete', 'c');
      expect(component.pools.length).toBe(3);
      expectItemTasks(component.pools[0], 'Creating..., Updating..., Deleting');
      expectItemTasks(component.pools[1], 'Updating..., Deleting');
      expectItemTasks(component.pools[2], 'Deleting');
    });

    it('gets all pools with multiple executing tasks (not only pool tasks)', () => {
      addTask('rbd/create', 'a');
      addTask('rbd/edit', 'a');
      addTask('pool/delete', 'a');
      addTask('pool/edit', 'b');
      addTask('rbd/delete', 'b');
      addTask('rbd/delete', 'c');
      expect(component.pools.length).toBe(3);
      expectItemTasks(component.pools[0], 'Deleting');
      expectItemTasks(component.pools[1], 'Updating');
      expect(component.pools[2].cdExecuting).toBeFalsy();
    });
  });

  describe('getPgStatusCellClass', () => {
    const testMethod = (value: string, expected: string) =>
      expect(component.getPgStatusCellClass('', '', value)).toEqual({
        'text-right': true,
        [expected]: true
      });

    it('pg-clean', () => {
      testMethod('8 active+clean', 'pg-clean');
    });

    it('pg-working', () => {
      testMethod('  8 active+clean+scrubbing+deep, 255 active+clean  ', 'pg-working');
    });

    it('pg-warning', () => {
      testMethod('8 active+clean+scrubbing+down', 'pg-warning');
      testMethod('8 active+clean+scrubbing+down+nonMappedState', 'pg-warning');
    });

    it('pg-unknown', () => {
      testMethod('8 active+clean+scrubbing+nonMappedState', 'pg-unknown');
      testMethod('8 ', 'pg-unknown');
      testMethod('', 'pg-unknown');
    });
  });

  describe('transformPoolsData', () => {
    let pool: Pool;

    const getPoolData = (o: object) => [
      _.merge(
        _.merge(Mocks.getPool('a', 0), {
          application_metadata: ['Block'],
          cdIsBinary: true,
          migrationTag: null,
          pg_status: '',
          stats: {
            bytes_used: { latest: 0, rate: 0, rates: [] },
            max_avail: { latest: 0, rate: 0, rates: [] },
            avail_raw: { latest: 0, rate: 0, rates: [] },
            percent_used: { latest: 0, rate: 0, rates: [] },
            rd: { latest: 0, rate: 0, rates: [] },
            rd_bytes: { latest: 0, rate: 0, rates: [] },
            wr: { latest: 0, rate: 0, rates: [] },
            wr_bytes: { latest: 0, rate: 0, rates: [] }
          },
          usage: 0,
          data_protection: 'replica: ×3'
        }),
        o
      )
    ];

    beforeEach(() => {
      pool = Mocks.getPool('a', 0);
    });

    it('transforms replicated pools data correctly', () => {
      pool = _.merge(pool, {
        stats: {
          bytes_used: { latest: 5, rate: 0, rates: [] },
          avail_raw: { latest: 15, rate: 0, rates: [] },
          percent_used: { latest: 0.25, rate: 0, rates: [] },
          rd_bytes: {
            latest: 6,
            rate: 4,
            rates: [
              [0, 2],
              [1, 6]
            ]
          }
        },
        pg_status: { 'active+clean': 8, down: 2 }
      });
      expect(component.transformPoolsData([pool])).toEqual(
        getPoolData({
          pg_status: '8 active+clean, 2 down',
          stats: {
            bytes_used: { latest: 5, rate: 0, rates: [] },
            avail_raw: { latest: 15, rate: 0, rates: [] },
            percent_used: { latest: 0.25, rate: 0, rates: [] },
            rd_bytes: { latest: 6, rate: 4, rates: [2, 6] }
          },
          usage: 0.25,
          data_protection: 'replica: ×3'
        })
      );
    });

    it('transforms erasure pools data correctly', () => {
      pool.type = 'erasure';
      pool.erasure_code_profile = 'default';
      component.ecProfileList = getECPProfiles();

      expect(component.transformPoolsData([pool])).toEqual(
        getPoolData({
          type: 'erasure',
          erasure_code_profile: 'default',
          data_protection: 'EC: 2+1'
        })
      );
    });

    it('transforms pools data correctly if stats are missing', () => {
      expect(component.transformPoolsData([pool])).toEqual(getPoolData({}));
    });

    it('transforms empty pools data correctly', () => {
      expect(component.transformPoolsData(undefined)).toEqual(undefined);
      expect(component.transformPoolsData([])).toEqual([]);
    });

    it('shows not marked pools in progress if pg_num does not match pg_num_target', () => {
      const pools = [
        _.merge(pool, {
          pg_num: 32,
          pg_num_target: 16,
          pg_placement_num: 32,
          pg_placement_num_target: 16
        })
      ];
      expect(component.transformPoolsData(pools)).toEqual(
        getPoolData({
          cdExecuting: 'Updating',
          pg_num: 32,
          pg_num_target: 16,
          pg_placement_num: 32,
          pg_placement_num_target: 16,
          data_protection: 'replica: ×3'
        })
      );
    });

    it('shows marked pools in progress as defined by task', () => {
      const pools = [
        _.merge(pool, {
          pg_num: 32,
          pg_num_target: 16,
          pg_placement_num: 32,
          pg_placement_num_target: 16,
          cdExecuting: 'Updating... 50%'
        })
      ];
      expect(component.transformPoolsData(pools)).toEqual(
        getPoolData({
          cdExecuting: 'Updating... 50%',
          pg_num: 32,
          pg_num_target: 16,
          pg_placement_num: 32,
          pg_placement_num_target: 16,
          data_protection: 'replica: ×3'
        })
      );
    });
  });

  describe('transformPgStatus', () => {
    it('returns status groups correctly', () => {
      const pgStatus = { 'active+clean': 8 };
      const expected = '8 active+clean';

      expect(transformPgStatus(pgStatus)).toEqual(expected);
    });

    it('returns separated status groups', () => {
      const pgStatus = { 'active+clean': 8, down: 2 };
      const expected = '8 active+clean, 2 down';

      expect(transformPgStatus(pgStatus)).toEqual(expected);
    });

    it('returns separated statuses correctly', () => {
      const pgStatus = { active: 8, down: 2 };
      const expected = '8 active, 2 down';

      expect(transformPgStatus(pgStatus)).toEqual(expected);
    });

    it('returns empty string', () => {
      const pgStatus: any = undefined;
      const expected = '';

      expect(transformPgStatus(pgStatus)).toEqual(expected);
    });
  });

  describe('getSelectionTiers', () => {
    const setSelectionTiers = (tiers: number[]) => {
      component.expandedRow = { tiers };
      component.getSelectionTiers();
    };

    beforeEach(() => {
      component.pools = getPoolList();
    });

    it('should select multiple existing cache tiers', () => {
      setSelectionTiers([0, 1, 2]);
      expect(component.cacheTiers).toEqual(getPoolList());
    });

    it('should select correct existing cache tier', () => {
      setSelectionTiers([0]);
      expect(component.cacheTiers).toEqual([Mocks.getPool('a', 0)]);
    });

    it('should not select cache tier if id is invalid', () => {
      setSelectionTiers([-1]);
      expect(component.cacheTiers).toEqual([]);
    });

    it('should not select cache tier if empty', () => {
      setSelectionTiers([]);
      expect(component.cacheTiers).toEqual([]);
    });

    it('should be able to selected one pool with multiple tiers, than with a single tier, than with no tiers', () => {
      setSelectionTiers([0, 1, 2]);
      expect(component.cacheTiers).toEqual(getPoolList());
      setSelectionTiers([0]);
      expect(component.cacheTiers).toEqual([Mocks.getPool('a', 0)]);
      setSelectionTiers([]);
      expect(component.cacheTiers).toEqual([]);
    });
  });

  describe('enablePoolDeletion', () => {
    let configurationService: ConfigurationService;

    beforeEach(() => {
      configurationService = TestBed.inject(ConfigurationService);
    });

    it('should call configurationService.create and update monAllowPoolDelete$', () => {
      spyOn(configurationService, 'create').and.returnValue(of(null));
      component.monAllowPoolDelete$.next(false);
      component.enablePoolDeletion();
      expect(configurationService.create).toHaveBeenCalledWith({
        name: 'mon_allow_pool_delete',
        value: [{ section: 'mon', value: true }],
        force_update: false
      });
      expect(component.monAllowPoolDelete$.value).toBe(true);
    });

    it('should not update monAllowPoolDelete$ on error', () => {
      spyOn(configurationService, 'create').and.returnValue(throwError(() => 'fail'));
      component.monAllowPoolDelete$.next(false);
      component.enablePoolDeletion();
      expect(component.monAllowPoolDelete$.value).toBe(false);
    });
  });

  describe('isReleaseAtLeast', () => {
    it('should return true when current equals minimum', () => {
      expect(isReleaseAtLeast('umbrella', 'umbrella')).toBe(true);
    });

    it('should return true when current is newer than minimum', () => {
      expect(isReleaseAtLeast('umbrella', 'squid')).toBe(true);
    });

    it('should return false when current is older than minimum', () => {
      expect(isReleaseAtLeast('reef', 'umbrella')).toBe(false);
    });

    it('should return false for unknown release names', () => {
      expect(isReleaseAtLeast('unknown', 'umbrella')).toBe(false);
    });

    it('should be case-insensitive', () => {
      expect(isReleaseAtLeast('Umbrella', 'UMBRELLA')).toBe(true);
    });
  });

  describe('pool migration flag helpers', () => {
    it('should detect the flag in a list or wildcard', () => {
      expect(isPoolMigrationFlagEnabled('*')).toBe(true);
      expect(isPoolMigrationFlagEnabled('poolmigration')).toBe(true);
      expect(isPoolMigrationFlagEnabled('other, poolmigration')).toBe(true);
    });

    it('should not detect the flag when it is missing or the value is not a string', () => {
      expect(isPoolMigrationFlagEnabled('other')).toBe(false);
      expect(isPoolMigrationFlagEnabled('')).toBe(false);
      expect(isPoolMigrationFlagEnabled(undefined)).toBe(false);
    });

    it('should add the flag preserving existing features', () => {
      expect(addPoolMigrationFlag(undefined)).toBe('poolmigration');
      expect(addPoolMigrationFlag('')).toBe('poolmigration');
      expect(addPoolMigrationFlag('other')).toBe('other,poolmigration');
      expect(addPoolMigrationFlag('other, another')).toBe('other,another,poolmigration');
    });

    it('should keep the value untouched when the flag is already enabled', () => {
      expect(addPoolMigrationFlag('*')).toBe('*');
      expect(addPoolMigrationFlag('poolmigration')).toBe('poolmigration');
    });
  });

  describe('getPoolBusyReason', () => {
    const pool = (changes: Partial<Pool> = {}): Pool => ({
      ...Mocks.getPool('test', 1),
      ...changes
    });

    it('should return null for an idle pool', () => {
      expect(getPoolBusyReason(pool())).toBeNull();
    });

    it('should detect a pool that is the source or target of a migration', () => {
      expect(getPoolBusyReason(pool({ migration_src: 0 }))).toBe(PoolBusyReason.MIGRATING);
      expect(getPoolBusyReason(pool({ migration_target: 2 }))).toBe(PoolBusyReason.MIGRATING);
    });

    it('should detect a pool whose PG count is being adjusted', () => {
      expect(getPoolBusyReason(pool({ pg_num: 32, pg_num_target: 64 }))).toBe(
        PoolBusyReason.PG_CHANGE
      );
      expect(getPoolBusyReason(pool({ pg_placement_num: 32, pg_placement_num_target: 64 }))).toBe(
        PoolBusyReason.PG_CHANGE
      );
    });

    it('should detect any other running task', () => {
      expect(getPoolBusyReason(pool({ cdExecuting: 'Updating...' }))).toBe(PoolBusyReason.TASK);
    });

    it('should prefer the specific cause over the generic running task', () => {
      expect(getPoolBusyReason(pool({ migration_src: 0, cdExecuting: 'Migrating' }))).toBe(
        PoolBusyReason.MIGRATING
      );
      expect(
        getPoolBusyReason(pool({ pg_num: 32, pg_num_target: 64, cdExecuting: 'Updating' }))
      ).toBe(PoolBusyReason.PG_CHANGE);
    });
  });

  describe('migratePool prerequisites', () => {
    let modalShow: jest.SpyInstance;
    let navigate: jest.SpyInstance;
    let configurationService: ConfigurationService;

    const flagConfig = (value?: string) => ({
      value: value === undefined ? [] : [{ section: 'global', value }]
    });
    const umbrellaInfo = {
      require_osd_release: 'umbrella',
      require_min_compat_client: 'umbrella'
    };

    beforeEach(() => {
      modalShow = jest.spyOn(TestBed.inject(ModalCdsService), 'show').mockImplementation();
      navigate = jest.spyOn(TestBed.inject(Router), 'navigate').mockResolvedValue(true);
      configurationService = TestBed.inject(ConfigurationService);
      component.selection.selected = [Mocks.getPool('test pool', 1)];
    });

    const mockCluster = (info: object, config: object) => {
      spyOn(poolService, 'getInfo').and.returnValue(of(info));
      spyOn(configurationService, 'get').and.returnValue(of(config));
    };

    it('should keep the migrate, edit, and delete actions enabled regardless of cluster state', () => {
      const migrateAction = component.tableActions.find(
        (a) => a.name === component.actionLabels.MIGRATE
      );
      const editAction = component.tableActions.find((a) => a.name === component.actionLabels.EDIT);
      const deleteAction = component.tableActions.find(
        (a) => a.name === component.actionLabels.DELETE
      );
      expect(migrateAction.disable).toBeUndefined();
      expect(editAction.disable).toBeUndefined();
      expect(deleteAction.disable).toBeUndefined();
    });

    it('should show the OSD release modal when require_osd_release is older than umbrella', () => {
      mockCluster({ ...umbrellaInfo, require_osd_release: 'tentacle' }, flagConfig());
      component.migratePool();
      expect(modalShow).toHaveBeenCalledWith(PoolMigrationOsdReleaseModalComponent);
      expect(navigate).not.toHaveBeenCalled();
    });

    it('should check the OSD release before the other prerequisites', () => {
      mockCluster({}, flagConfig());
      component.migratePool();
      expect(modalShow).toHaveBeenCalledTimes(1);
      expect(modalShow).toHaveBeenCalledWith(PoolMigrationOsdReleaseModalComponent);
      expect(configurationService.get).not.toHaveBeenCalled();
    });

    it('should ask for both settings when the flag and min compat client are missing', () => {
      mockCluster(
        { require_osd_release: 'umbrella', require_min_compat_client: 'reef' },
        flagConfig()
      );
      component.migratePool();
      expect(modalShow).toHaveBeenCalledWith(
        PoolMigrationClusterSettingsModalComponent,
        expect.objectContaining({ needsFlag: true, needsMinCompatClient: true })
      );
      expect(navigate).not.toHaveBeenCalled();
    });

    it('should only ask for the flag when min compat client is already set', () => {
      mockCluster(umbrellaInfo, flagConfig('other'));
      component.migratePool();
      expect(modalShow).toHaveBeenCalledWith(
        PoolMigrationClusterSettingsModalComponent,
        expect.objectContaining({
          needsFlag: true,
          needsMinCompatClient: false,
          flagValue: 'other'
        })
      );
    });

    it('should only ask for min compat client when the flag is already enabled', () => {
      mockCluster(
        { require_osd_release: 'umbrella', require_min_compat_client: 'reef' },
        flagConfig('poolmigration')
      );
      component.migratePool();
      expect(modalShow).toHaveBeenCalledWith(
        PoolMigrationClusterSettingsModalComponent,
        expect.objectContaining({ needsFlag: false, needsMinCompatClient: true })
      );
    });

    it('should navigate to the migrate form when the modal completes', () => {
      mockCluster(umbrellaInfo, flagConfig());
      component.migratePool();
      const { onComplete } = modalShow.mock.calls[0][1];
      expect(navigate).not.toHaveBeenCalled();
      onComplete();
      expect(navigate).toHaveBeenCalledWith(['/pool/migrate', 'test%20pool']);
    });

    it('should navigate to the migrate form when every prerequisite is met', () => {
      mockCluster(umbrellaInfo, flagConfig('poolmigration'));
      component.migratePool();
      expect(modalShow).not.toHaveBeenCalled();
      expect(navigate).toHaveBeenCalledWith(['/pool/migrate', 'test%20pool']);
    });

    describe('pool state prerequisites', () => {
      const selectPool = (changes: Partial<Pool>) =>
        (component.selection.selected = [{ ...Mocks.getPool('test pool', 1), ...changes }]);

      it('should show the busy modal when the pool is already migrating', () => {
        mockCluster(umbrellaInfo, flagConfig('poolmigration'));
        selectPool({ migration_src: 0, cdExecuting: 'Migrating' });
        component.migratePool();
        expect(modalShow).toHaveBeenCalledWith(PoolBusyModalComponent, {
          reason: PoolBusyReason.MIGRATING,
          taskDescription: 'Migrating',
          action: PoolAction.MIGRATE
        });
        expect(navigate).not.toHaveBeenCalled();
      });

      it('should show the busy modal when the PG count is changing', () => {
        mockCluster(umbrellaInfo, flagConfig('poolmigration'));
        selectPool({ pg_num: 32, pg_num_target: 64 });
        component.migratePool();
        expect(modalShow).toHaveBeenCalledWith(
          PoolBusyModalComponent,
          expect.objectContaining({ reason: PoolBusyReason.PG_CHANGE, action: PoolAction.MIGRATE })
        );
        expect(navigate).not.toHaveBeenCalled();
      });

      it('should show the busy modal with the task description for any other task', () => {
        mockCluster(umbrellaInfo, flagConfig('poolmigration'));
        selectPool({ cdExecuting: 'Deleting...' });
        component.migratePool();
        expect(modalShow).toHaveBeenCalledWith(PoolBusyModalComponent, {
          reason: PoolBusyReason.TASK,
          taskDescription: 'Deleting...',
          action: PoolAction.MIGRATE
        });
      });

      it('should report cluster prerequisites first, even if the pool is busy', () => {
        mockCluster(umbrellaInfo, flagConfig());
        selectPool({ migration_src: 0 });
        component.migratePool();
        expect(modalShow).toHaveBeenCalledTimes(1);
        expect(modalShow).toHaveBeenCalledWith(
          PoolMigrationClusterSettingsModalComponent,
          expect.anything()
        );
      });

      it('should check the pool again once the cluster settings modal completes', () => {
        mockCluster(umbrellaInfo, flagConfig());
        selectPool({ pg_num: 32, pg_num_target: 64 });
        component.migratePool();
        const { onComplete } = modalShow.mock.calls[0][1];
        onComplete();
        expect(modalShow).toHaveBeenLastCalledWith(
          PoolBusyModalComponent,
          expect.objectContaining({ reason: PoolBusyReason.PG_CHANGE, action: PoolAction.MIGRATE })
        );
        expect(navigate).not.toHaveBeenCalled();
      });
    });
  });

  describe('editPool', () => {
    let modalShow: jest.SpyInstance;
    let navigate: jest.SpyInstance;

    beforeEach(() => {
      modalShow = jest.spyOn(TestBed.inject(ModalCdsService), 'show').mockImplementation();
      navigate = jest.spyOn(TestBed.inject(Router), 'navigate').mockResolvedValue(true);
    });

    const selectPool = (changes: Partial<Pool> = {}) =>
      (component.selection.selected = [{ ...Mocks.getPool('test pool', 1), ...changes }]);

    it('should navigate to the edit form when the pool is idle', () => {
      selectPool();
      component.editPool();
      expect(modalShow).not.toHaveBeenCalled();
      expect(navigate).toHaveBeenCalledWith(['/pool/edit', 'test%20pool']);
    });

    it('should show the busy modal when the pool is migrating', () => {
      selectPool({ migration_src: 0, cdExecuting: 'Migrating' });
      component.editPool();
      expect(modalShow).toHaveBeenCalledWith(PoolBusyModalComponent, {
        reason: PoolBusyReason.MIGRATING,
        taskDescription: 'Migrating',
        action: PoolAction.EDIT
      });
      expect(navigate).not.toHaveBeenCalled();
    });

    it('should show the busy modal when PG count is changing', () => {
      selectPool({ pg_num: 32, pg_num_target: 64 });
      component.editPool();
      expect(modalShow).toHaveBeenCalledWith(PoolBusyModalComponent, {
        reason: PoolBusyReason.PG_CHANGE,
        taskDescription: undefined,
        action: PoolAction.EDIT
      });
      expect(navigate).not.toHaveBeenCalled();
    });

    it('should show the busy modal for any other running task', () => {
      selectPool({ cdExecuting: 'Updating...' });
      component.editPool();
      expect(modalShow).toHaveBeenCalledWith(PoolBusyModalComponent, {
        reason: PoolBusyReason.TASK,
        taskDescription: 'Updating...',
        action: PoolAction.EDIT
      });
      expect(navigate).not.toHaveBeenCalled();
    });
  });

  describe('deletePoolModal with busy pool', () => {
    let modalShow: jest.SpyInstance;

    beforeEach(() => {
      modalShow = jest.spyOn(TestBed.inject(ModalCdsService), 'show').mockImplementation();
    });

    const selectPool = (changes: Partial<Pool> = {}) =>
      (component.selection.selected = [{ ...Mocks.getPool('test pool', 1), ...changes }]);

    it('should show the busy modal when the pool is migrating', () => {
      selectPool({ migration_src: 0, cdExecuting: 'Migrating' });
      component.deletePoolModal();
      expect(modalShow).toHaveBeenCalledWith(PoolBusyModalComponent, {
        reason: PoolBusyReason.MIGRATING,
        taskDescription: 'Migrating',
        action: PoolAction.DELETE
      });
    });

    it('should show the busy modal when PG count is changing', () => {
      selectPool({ pg_num: 32, pg_num_target: 64 });
      component.deletePoolModal();
      expect(modalShow).toHaveBeenCalledWith(PoolBusyModalComponent, {
        reason: PoolBusyReason.PG_CHANGE,
        taskDescription: undefined,
        action: PoolAction.DELETE
      });
    });

    it('should show the busy modal for any other running task', () => {
      selectPool({ cdExecuting: 'Updating...' });
      component.deletePoolModal();
      expect(modalShow).toHaveBeenCalledWith(PoolBusyModalComponent, {
        reason: PoolBusyReason.TASK,
        taskDescription: 'Updating...',
        action: PoolAction.DELETE
      });
    });

    it('should show delete confirmation when the pool is idle and deletion is allowed', () => {
      component.monAllowPoolDelete$.next(true);
      selectPool();
      component.deletePoolModal();
      expect(modalShow).toHaveBeenCalledWith(
        DeleteConfirmationModalComponent,
        expect.objectContaining({ itemNames: ['test pool'] })
      );
    });

    it('should show confirmation modal to enable deletion when monAllowPoolDelete is false and user has perms', () => {
      component.monAllowPoolDelete$.next(false);
      component.permissions.configOpt.read = true;
      component.permissions.pool.read = true;
      selectPool();
      component.deletePoolModal();
      expect(modalShow).toHaveBeenCalledWith(
        ConfirmationModalComponent,
        expect.objectContaining({ titleText: "Can't delete test pool" })
      );
    });

    it('should show delete guard modal when monAllowPoolDelete is false and user has no perms', () => {
      component.monAllowPoolDelete$.next(false);
      component.permissions.configOpt.read = false;
      selectPool();
      component.deletePoolModal();
      expect(modalShow).toHaveBeenCalledWith(
        DeleteGuardModalComponent,
        expect.objectContaining({ resourceName: 'test pool' })
      );
    });
  });

  describe('transformPoolsData with migration', () => {
    it('should set cdExecuting to Migrating when pool has migration_src', () => {
      const pool = _.merge(Mocks.getPool('target', 1), {
        migration_src: 0,
        stats: {}
      });
      const result = component.transformPoolsData([pool]);
      expect(result[0].cdExecuting).toBe('Migrating');
    });

    it('should set cdExecuting to Migrating when pool has migration_target', () => {
      const pool = _.merge(Mocks.getPool('source', 0), {
        migration_target: 1,
        stats: {}
      });
      const result = component.transformPoolsData([pool]);
      expect(result[0].cdExecuting).toBe('Migrating');
    });

    it('should not override cdExecuting if already set by a task', () => {
      const pool = _.merge(Mocks.getPool('target', 1), {
        migration_src: 0,
        cdExecuting: 'Updating... 50%',
        stats: {}
      });
      const result = component.transformPoolsData([pool]);
      expect(result[0].cdExecuting).toBe('Updating... 50%');
    });
  });
});
