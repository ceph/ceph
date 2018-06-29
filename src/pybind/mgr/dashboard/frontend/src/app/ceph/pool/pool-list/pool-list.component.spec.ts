import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalService, TabsModule } from 'ngx-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { PoolService } from '../../../shared/api/pool.service';
import { DeletionModalComponent } from '../../../shared/components/deletion-modal/deletion-modal.component';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { SharedModule } from '../../../shared/shared.module';
import { PoolListComponent } from './pool-list.component';

describe('PoolListComponent', () => {
  let component: PoolListComponent;
  let fixture: ComponentFixture<PoolListComponent>;

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
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('pool deletion', () => {
    let poolService: PoolService;
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
      poolService = TestBed.get(PoolService);
      spyOn(poolService, 'delete').and.stub();
      taskWrapper = TestBed.get(TaskWrapperService);
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
    });

    it('should pool deletion with two different pools', () => {
      testPoolDeletion('somePoolName');
      testPoolDeletion('aDifferentPoolName');
    });
  });
});
