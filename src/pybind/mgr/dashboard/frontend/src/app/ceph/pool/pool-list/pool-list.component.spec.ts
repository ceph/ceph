import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { configureTestBed, expectItemTasks } from '../../../../testing/unit-test-helper';
import { ConfigurationService } from '../../../shared/api/configuration.service';
import { PoolService } from '../../../shared/api/pool.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ModalService } from '../../../shared/services/modal.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdConfigurationListComponent } from '../../block/rbd-configuration-list/rbd-configuration-list.component';
import { PgCategoryService } from '../../shared/pg-category.service';
import { Pool } from '../pool';
import { PoolDetailsComponent } from '../pool-details/pool-details.component';
import { PoolListComponent } from './pool-list.component';

describe('PoolListComponent', () => {
  let component: PoolListComponent;
  let fixture: ComponentFixture<PoolListComponent>;
  let poolService: PoolService;

  const createPool = (name: string, id: number): Pool => {
    return _.merge(new Pool(name), {
      pool: id,
      pg_num: 256,
      pg_placement_num: 256,
      pg_num_target: 256,
      pg_placement_num_target: 256
    });
  };

  const getPoolList = (): Pool[] => {
    return [createPool('a', 0), createPool('b', 1), createPool('c', 2)];
  };

  configureTestBed({
    declarations: [PoolListComponent, PoolDetailsComponent, RbdConfigurationListComponent],
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      ToastrModule.forRoot(),
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
        configOpt: { read: configOptRead }
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
      expect(component.monAllowPoolDelete).toBe(true);
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
      expect(component.monAllowPoolDelete).toBe(false);
    });

    it('should set value correctly if mon_allow_pool_delete flag is not set', () => {
      const configOption = {
        name: 'mon_allow_pool_delete'
      };
      spyOn(configurationService, 'get').and.returnValue(of(configOption));
      fixture = TestBed.createComponent(PoolListComponent);
      component = fixture.componentInstance;
      expect(component.monAllowPoolDelete).toBe(false);
    });

    it('should set value correctly w/o config-opt read privileges', () => {
      configOptRead = false;
      fixture = TestBed.createComponent(PoolListComponent);
      component = fixture.componentInstance;
      expect(component.monAllowPoolDelete).toBe(false);
    });
  });

  describe('pool deletion', () => {
    let taskWrapper: TaskWrapperService;

    const setSelectedPool = (poolName: string) =>
      (component.selection.selected = [{ pool_name: poolName }]);

    const callDeletion = () => {
      component.deletePoolModal();
      const deletion: CriticalConfirmationModalComponent = component.modalRef.componentInstance;
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
        return {
          componentInstance: Object.assign(new deletionClass(), initialState)
        };
      });
      spyOn(poolService, 'delete').and.stub();
      taskWrapper = TestBed.inject(TaskWrapperService);
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
    });

    it('should pool deletion with two different pools', () => {
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

  describe('custom row comparators', () => {
    const expectCorrectComparator = (statsAttribute: string) => {
      const mockPool = (v: number) => ({ stats: { [statsAttribute]: { latest: v } } });
      const columnDefinition = _.find(
        component.columns,
        (column) => column.prop === `stats.${statsAttribute}.rates`
      );
      expect(columnDefinition.comparator(undefined, undefined, mockPool(2), mockPool(1))).toBe(1);
      expect(columnDefinition.comparator(undefined, undefined, mockPool(1), mockPool(2))).toBe(-1);
    };

    it('compares read bytes correctly', () => {
      expectCorrectComparator('rd_bytes');
    });

    it('compares write bytes correctly', () => {
      expectCorrectComparator('wr_bytes');
    });
  });

  describe('transformPoolsData', () => {
    let pool: Pool;

    const getPoolData = (o: object) => [
      _.merge(
        _.merge(createPool('a', 0), {
          cdIsBinary: true,
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
          usage: 0
        }),
        o
      )
    ];

    beforeEach(() => {
      pool = createPool('a', 0);
    });

    it('transforms pools data correctly', () => {
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
          usage: 0.25
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
          pg_placement_num_target: 16
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
          pg_placement_num_target: 16
        })
      );
    });
  });

  describe('transformPgStatus', () => {
    it('returns status groups correctly', () => {
      const pgStatus = { 'active+clean': 8 };
      const expected = '8 active+clean';

      expect(component.transformPgStatus(pgStatus)).toEqual(expected);
    });

    it('returns separated status groups', () => {
      const pgStatus = { 'active+clean': 8, down: 2 };
      const expected = '8 active+clean, 2 down';

      expect(component.transformPgStatus(pgStatus)).toEqual(expected);
    });

    it('returns separated statuses correctly', () => {
      const pgStatus = { active: 8, down: 2 };
      const expected = '8 active, 2 down';

      expect(component.transformPgStatus(pgStatus)).toEqual(expected);
    });

    it('returns empty string', () => {
      const pgStatus: any = undefined;
      const expected = '';

      expect(component.transformPgStatus(pgStatus)).toEqual(expected);
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
      expect(component.cacheTiers).toEqual([createPool('a', 0)]);
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
      expect(component.cacheTiers).toEqual([createPool('a', 0)]);
      setSelectionTiers([]);
      expect(component.cacheTiers).toEqual([]);
    });
  });

  describe('getDisableDesc', () => {
    it('should return message if mon_allow_pool_delete flag is set to false', () => {
      component.monAllowPoolDelete = false;
      expect(component.getDisableDesc()).toBe(
        'Pool deletion is disabled by the mon_allow_pool_delete configuration setting.'
      );
    });

    it('should return undefined if mon_allow_pool_delete flag is set to true', () => {
      component.monAllowPoolDelete = true;
      expect(component.getDisableDesc()).toBeUndefined();
    });
  });
});
