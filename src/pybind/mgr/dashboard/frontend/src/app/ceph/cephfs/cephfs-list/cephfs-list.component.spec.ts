import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { CephfsVolumeFormComponent } from '../cephfs-form/cephfs-form.component';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephfsListComponent } from './cephfs-list.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';

@Component({ selector: 'cd-cephfs-tabs', template: '' })
class CephfsTabsStubComponent {
  @Input()
  selection: CdTableSelection;
}

describe('CephfsListComponent', () => {
  let component: CephfsListComponent;
  let fixture: ComponentFixture<CephfsListComponent>;
  let cephfsService: CephfsService;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      HttpClientTestingModule,
      ToastrModule.forRoot(),
      RouterTestingModule
    ],
    declarations: [CephfsListComponent, CephfsTabsStubComponent, CephfsVolumeFormComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsListComponent);
    component = fixture.componentInstance;
    cephfsService = TestBed.inject(CephfsService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('volume deletion', () => {
    let taskWrapper: TaskWrapperService;
    let modalRef: any;

    const setSelectedVolume = (volName: string) =>
      (component.selection.selected = [{ mdsmap: { fs_name: volName } }]);

    const callDeletion = () => {
      component.removeVolumeModal();
      expect(modalRef).toBeTruthy();
      const deletion: CriticalConfirmationModalComponent = modalRef && modalRef.componentInstance;
      deletion.submitActionObservable();
    };

    const testVolumeDeletion = (volName: string) => {
      setSelectedVolume(volName);
      callDeletion();
      expect(cephfsService.remove).toHaveBeenCalledWith(volName);
      expect(taskWrapper.wrapTaskAroundCall).toHaveBeenCalledWith({
        task: {
          name: 'cephfs/remove',
          metadata: {
            volumeName: volName
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
      spyOn(cephfsService, 'remove').and.stub();
      taskWrapper = TestBed.inject(TaskWrapperService);
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
    });

    it('should delete cephfs volume', () => {
      testVolumeDeletion('somevolumeName');
    });
  });
});
