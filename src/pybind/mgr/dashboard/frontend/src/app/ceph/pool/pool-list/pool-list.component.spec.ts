import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalService } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { PoolService } from '../../../shared/api/pool.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ExecutingTask } from '../../../shared/models/executing-task';
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

  const addPool = (pools, name, id) => {
    const pool = new Pool(name);
    pool.pool = id;
    pool.pg_num = 256;
    pools.push(pool);
  };

  const setUpPools = (pools) => {
    addPool(pools, 'a', 0);
    addPool(pools, 'b', 1);
    addPool(pools, 'c', 2);
    component.pools = pools;
  };

  configureTestBed({
    declarations: [PoolListComponent, PoolDetailsComponent, RbdConfigurationListComponent],
    imports: [
      SharedModule,
      ToastModule.forRoot(),
      RouterTestingModule,
      TabsModule.forRoot(),
      HttpClientTestingModule
    ],
    providers: [i18nProviders, PgCategoryService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolListComponent);
    component = fixture.componentInstance;
    component.permissions.pool.read = true;
    poolService = TestBed.get(PoolService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('pool deletion', () => {
    let taskWrapper: TaskWrapperService;

    const setSelectedPool = (poolName: string) => {
      component.selection.selected = [{ pool_name: poolName }];
      component.selection.update();
    };

    const callDeletion = () => {
      component.deletePoolModal();
      const deletion: CriticalConfirmationModalComponent = component.modalRef.content;
      deletion.submitActionObservable();
    };

    const testPoolDeletion = (poolName) => {
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
      spyOn(TestBed.get(BsModalService), 'show').and.callFake((deletionClass, config) => {
        return {
          content: Object.assign(new deletionClass(), config.initialState)
        };
      });
      spyOn(poolService, 'delete').and.stub();
      taskWrapper = TestBed.get(TaskWrapperService);
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
    });

    it('should pool deletion with two different pools', () => {
      testPoolDeletion('somePoolName');
      testPoolDeletion('aDifferentPoolName');
    });
  });

  describe('handling of executing tasks', () => {
    let pools: Pool[];
    let summaryService: SummaryService;

    const addTask = (name: string, pool: string) => {
      const task = new ExecutingTask();
      task.name = name;
      task.metadata = { pool_name: pool };
      summaryService.addRunningTask(task);
    };

    beforeEach(() => {
      summaryService = TestBed.get(SummaryService);
      summaryService['summaryDataSource'].next({ executing_tasks: [], finished_tasks: [] });
      pools = [];
      setUpPools(pools);
      spyOn(poolService, 'getList').and.callFake(() => of(pools));
      fixture.detectChanges();
    });

    it('gets all pools without executing pools', () => {
      expect(component.pools.length).toBe(3);
      expect(component.pools.every((pool) => !pool.executingTasks)).toBeTruthy();
    });

    it('gets a pool from a task during creation', () => {
      addTask('pool/create', 'd');
      expect(component.pools.length).toBe(4);
      expect(component.pools[3].cdExecuting).toBe('Creating');
    });

    it('gets all pools with one executing pools', () => {
      addTask('pool/create', 'a');
      expect(component.pools.length).toBe(3);
      expect(component.pools[0].cdExecuting).toBe('Creating');
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
      expect(component.pools[0].cdExecuting).toBe('Creating, Updating, Deleting');
      expect(component.pools[1].cdExecuting).toBe('Updating, Deleting');
      expect(component.pools[2].cdExecuting).toBe('Deleting');
    });

    it('gets all pools with multiple executing tasks (not only pool tasks)', () => {
      addTask('rbd/create', 'a');
      addTask('rbd/edit', 'a');
      addTask('pool/delete', 'a');
      addTask('pool/edit', 'b');
      addTask('rbd/delete', 'b');
      addTask('rbd/delete', 'c');
      expect(component.pools.length).toBe(3);
      expect(component.pools[0].cdExecuting).toBe('Deleting');
      expect(component.pools[1].cdExecuting).toBe('Updating');
      expect(component.pools[2].cdExecuting).toBeFalsy();
    });
  });

  describe('getPgStatusCellClass', () => {
    const testMethod = (value, expected) =>
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
    it('transforms pools data correctly', () => {
      const pools = [
        {
          stats: { rd_bytes: { latest: 6, rate: 4, series: [[0, 2], [1, 6]] } },
          pg_status: { 'active+clean': 8, down: 2 }
        }
      ];
      const expected = [
        {
          cdIsBinary: true,
          pg_status: '8 active+clean, 2 down',
          stats: {
            bytes_used: { latest: 0, rate: 0, series: [] },
            max_avail: { latest: 0, rate: 0, series: [] },
            rd: { latest: 0, rate: 0, series: [] },
            rd_bytes: { latest: 6, rate: 4, series: [2, 6] },
            wr: { latest: 0, rate: 0, series: [] },
            wr_bytes: { latest: 0, rate: 0, series: [] }
          }
        }
      ];

      expect(component.transformPoolsData(pools)).toEqual(expected);
    });

    it('transforms empty pools data correctly', () => {
      const pools = undefined;
      const expected = undefined;

      expect(component.transformPoolsData(pools)).toEqual(expected);
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
      const pgStatus = undefined;
      const expected = '';

      expect(component.transformPgStatus(pgStatus)).toEqual(expected);
    });
  });

  describe('getPoolDetails', () => {
    it('returns pool details corretly', () => {
      const pool = { prop1: 1, cdIsBinary: true, prop2: 2, cdExecuting: true, prop3: 3 };
      const expected = { prop1: 1, prop2: 2, prop3: 3 };

      expect(component.getPoolDetails(pool)).toEqual(expected);
    });
  });

  describe('getSelectionTiers', () => {
    let pools: Pool[];
    const setSelectionTiers = (tiers: number[]) => {
      component.selection.selected = [
        {
          tiers
        }
      ];
      component.selection.update();
      component.getSelectionTiers();
    };

    beforeEach(() => {
      pools = [];
      setUpPools(pools);
    });

    it('should select multiple existing cache tiers', () => {
      setSelectionTiers([0, 1, 2]);
      expect(component.selectionCacheTiers).toEqual(pools);
    });

    it('should select correct existing cache tier', () => {
      setSelectionTiers([0]);
      expect(component.selectionCacheTiers).toEqual([{ pg_num: 256, pool: 0, pool_name: 'a' }]);
    });

    it('should not select cache tier if id is invalid', () => {
      setSelectionTiers([-1]);
      expect(component.selectionCacheTiers).toEqual([]);
    });

    it('should not select cache tier if empty', () => {
      setSelectionTiers([]);
      expect(component.selectionCacheTiers).toEqual([]);
    });

    it('should be able to selected one pool with multiple tiers, than with a single tier, than with no tiers', () => {
      setSelectionTiers([0, 1, 2]);
      expect(component.selectionCacheTiers).toEqual(pools);
      setSelectionTiers([0]);
      expect(component.selectionCacheTiers).toEqual([{ pg_num: 256, pool: 0, pool_name: 'a' }]);
      setSelectionTiers([]);
      expect(component.selectionCacheTiers).toEqual([]);
    });
  });
});
