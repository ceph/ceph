import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalService, TabsModule } from 'ngx-bootstrap';
import { of } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { PoolService } from '../../../shared/api/pool.service';
import { DeletionModalComponent } from '../../../shared/components/deletion-modal/deletion-modal.component';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { SharedModule } from '../../../shared/shared.module';
import { Pool } from '../pool';
import { PoolListComponent } from './pool-list.component';

describe('PoolListComponent', () => {
  let component: PoolListComponent;
  let fixture: ComponentFixture<PoolListComponent>;
  let poolService: PoolService;

  configureTestBed({
    declarations: [PoolListComponent],
    imports: [
      SharedModule,
      ToastModule.forRoot(),
      RouterTestingModule,
      TabsModule.forRoot(),
      HttpClientTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolListComponent);
    component = fixture.componentInstance;
    component.permission.read = true;
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
      const deletion: DeletionModalComponent = component.modalRef.content;
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

    const addPool = (name) => {
      const pool = new Pool(name);
      pool.pg_num = 256;
      pools.push(pool);
    };

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
      addPool('a');
      addPool('b');
      addPool('c');
      component.pools = pools;
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

    it('gets all pools with multiple executing tasks (not only pool tasks', () => {
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
});
