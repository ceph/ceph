import { HttpClientTestingModule } from '@angular/common/http/testing';
import {
  ComponentFixture,
  TestBed,
  discardPeriodicTasks,
  fakeAsync,
  tick
} from '@angular/core/testing';

import { CephfsSnapshotscheduleFormComponent } from './cephfs-snapshotschedule-form.component';

import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { FormHelper, configureTestBed } from '~/testing/unit-test-helper';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { of } from 'rxjs';
import {
  ModalService,
  ModalModule,
  InputModule,
  SelectModule,
  NumberModule
} from 'carbon-components-angular';
import { NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

describe('CephfsSnapshotscheduleFormComponent', () => {
  let component: CephfsSnapshotscheduleFormComponent;
  let fixture: ComponentFixture<CephfsSnapshotscheduleFormComponent>;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [CephfsSnapshotscheduleFormComponent],
    providers: [ModalService, { provide: 'fsName', useValue: 'test_fs' }],
    imports: [
      SharedModule,
      ReactiveFormsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      NgbTypeaheadModule,
      ModalModule,
      InputModule,
      SelectModule,
      NumberModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSnapshotscheduleFormComponent);
    component = fixture.componentInstance;
    component.fsName = 'test_fs';
    component.ngOnInit();
    formHelper = new FormHelper(component.snapScheduleForm);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a form open in modal', () => {
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cds-modal')).not.toBe(null);
  });

  it('should submit the form', fakeAsync(() => {
    const createSpy = spyOn(TestBed.inject(CephfsSnapshotScheduleService), 'create').and.stub();
    const checkScheduleExistsSpy = spyOn(
      TestBed.inject(CephfsSnapshotScheduleService),
      'checkScheduleExists'
    ).and.returnValue(of(false));
    const input = {
      directory: '/test',
      startDate: '2023-11-14 00:06:22',
      repeatInterval: 4,
      repeatFrequency: 'h'
    };

    formHelper.setMultipleValues(input);
    component.snapScheduleForm.get('directory').setValue('/test');
    component.submit();
    tick(400);

    expect(checkScheduleExistsSpy).toHaveBeenCalled();
    expect(createSpy).toHaveBeenCalled();
    discardPeriodicTasks();
  }));

  it('should skip schedule existence validation in embedded mode', fakeAsync(() => {
    component.embedded = true;
    component.hideDirectory = true;
    component.path = '/volumes/g1/sv1';
    component.ngOnInit();
    fixture.detectChanges();

    const checkScheduleExistsSpy = spyOn(
      TestBed.inject(CephfsSnapshotScheduleService),
      'checkScheduleExists'
    ).and.returnValue(of(true));

    component.snapScheduleForm.get('repeatInterval').setValue(2);
    component.snapScheduleForm.get('repeatFrequency').setValue('w');
    tick(400);
    component.snapScheduleForm.get('repeatFrequency').setValue('d');
    tick(400);

    expect(checkScheduleExistsSpy).not.toHaveBeenCalled();
    expect(component.snapScheduleForm.get('repeatFrequency').hasError('notUnique')).toBe(false);
    discardPeriodicTasks();
  }));
});
