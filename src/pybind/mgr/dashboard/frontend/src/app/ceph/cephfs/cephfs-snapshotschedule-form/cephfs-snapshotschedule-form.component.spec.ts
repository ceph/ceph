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
import { RepeatFrequency } from '~/app/shared/enum/repeat-frequency.enum';

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

  it('should use lowercase y for yearly snapshot schedules', () => {
    component.hideDirectory = true;
    component.fsName = 'test_fs';
    component.snapScheduleForm.get('repeatInterval').setValue(7);
    component.snapScheduleForm.get('repeatFrequency').setValue(RepeatFrequency.Yearly);
    formHelper.setMultipleValues({
      startDate: '2026-07-10 07:52:40'
    });

    const payload = component.buildCreatePayload(
      '/volumes/Group1/A1/954b0b54-3417-4f4b-844c-19494044b98a'
    );

    expect(payload.snap_schedule).toBe('7y');
  });

  it('should build per-path subvolume payload when embedded with multiple paths', () => {
    component.hideDirectory = true;
    component.fsName = 'test_fs';
    formHelper.setMultipleValues({
      startDate: '2023-11-14 00:06:22',
      repeatInterval: 3,
      repeatFrequency: 'w'
    });

    const firstPayload = component.buildCreatePayload(
      '/volumes/Group1/subvol1/688c8e86-b31a-49c0-a57c-4c27506c9d80'
    );
    const secondPayload = component.buildCreatePayload(
      '/volumes/Group1/subvol2/788c8e86-b31a-49c0-a57c-4c27506c9e81'
    );

    expect(firstPayload).toEqual(
      expect.objectContaining({
        fs: 'test_fs',
        path: '/volumes/Group1/subvol1/688c8e86-b31a-49c0-a57c-4c27506c9d80',
        subvol: 'subvol1',
        group: 'Group1'
      })
    );
    expect(secondPayload).toEqual(
      expect.objectContaining({
        fs: 'test_fs',
        path: '/volumes/Group1/subvol2/788c8e86-b31a-49c0-a57c-4c27506c9e81',
        subvol: 'subvol2',
        group: 'Group1'
      })
    );
  });

  it('should clear schedule uniqueness validation when interval changes', fakeAsync(() => {
    const snapScheduleService = TestBed.inject(CephfsSnapshotScheduleService);
    const checkScheduleExistsSpy = spyOn(snapScheduleService, 'checkScheduleExists').and.callFake(
      (_path, _fs, interval) => of(interval === 3)
    );

    component.hideDirectory = true;
    component.fsName = 'test_fs';
    component.snapScheduleForm.get('directory').setValue('/volumes/Group1/subvol1/uuid');
    component.snapScheduleForm.get('directory').disable();
    component.snapScheduleForm.get('repeatInterval').setValue(3);
    component.snapScheduleForm.get('repeatFrequency').setValue(RepeatFrequency.Monthly);

    component.onScheduleIntervalChange(3);
    tick(400);

    expect(checkScheduleExistsSpy).toHaveBeenCalledWith(
      '/volumes/Group1/subvol1/uuid',
      'test_fs',
      3,
      RepeatFrequency.Monthly,
      true
    );
    expect(component.snapScheduleForm.get('repeatFrequency')?.hasError('notUnique')).toBe(true);

    component.onScheduleIntervalChange(8);
    tick(400);

    expect(checkScheduleExistsSpy).toHaveBeenLastCalledWith(
      '/volumes/Group1/subvol1/uuid',
      'test_fs',
      8,
      RepeatFrequency.Monthly,
      true
    );
    expect(component.snapScheduleForm.get('repeatFrequency')?.hasError('notUnique')).toBe(false);
    expect(component.snapScheduleForm.get('repeatInterval')?.hasError('notUnique')).toBe(false);
    discardPeriodicTasks();
  }));

  it('should block submit when retention policy already exists for the path', fakeAsync(() => {
    const snapScheduleService = TestBed.inject(CephfsSnapshotScheduleService);
    const createSpy = spyOn(snapScheduleService, 'create').and.stub();
    spyOn(snapScheduleService, 'checkScheduleExists').and.returnValue(of(false));
    spyOn(snapScheduleService, 'checkRetentionPolicyExists').and.returnValue(
      of({ exists: true, errorIndex: 0, existingValue: 3 })
    );

    component.hideDirectory = true;
    component.fsName = 'test_fs';
    component.snapScheduleForm.get('directory').setValue('/volumes/Group1/subvol1/uuid');
    component.snapScheduleForm.get('directory').disable();
    component.addRetentionPolicy();
    component.retentionPolicies.at(0).get('retentionFrequency').setValue('M');
    formHelper.setMultipleValues({
      startDate: '2023-11-14 00:06:22',
      repeatInterval: 4,
      repeatFrequency: 'w'
    });

    component.submit();
    tick(400);

    expect(createSpy).not.toHaveBeenCalled();
    expect(
      component.retentionPolicies.at(0).get('retentionFrequency')?.hasError('notUnique')
    ).toBe(true);
    discardPeriodicTasks();
  }));

  it('should mark retention policy invalid when API reports an existing retention policy', () => {
    component.addRetentionPolicy();
    component.retentionPolicies.at(0).get('retentionFrequency').setValue('d');

    component.applyRetentionConflictFromDetail(
      'Failed to add retention policy for path /volumes/Group1/A1/uuid: Retention for d is already present with value 3. Please remove it first.'
    );

    expect(
      component.retentionPolicies.at(0).get('retentionFrequency')?.hasError('notUnique')
    ).toBe(true);
    expect(component.getRetentionConflictMessage(0)).toContain('3');
  });

  it('should invalidate form when retention frequency already exists on path', fakeAsync(() => {
    const snapScheduleService = TestBed.inject(CephfsSnapshotScheduleService);
    spyOn(snapScheduleService, 'checkRetentionPolicyExists').and.returnValue(
      of({ exists: true, errorIndex: 0, existingValue: 3 })
    );

    component.hideDirectory = true;
    component.fsName = 'test_fs';
    component.snapScheduleForm.get('directory').setValue('/volumes/Group1/subvol1/uuid');
    component.snapScheduleForm.get('directory').disable();
    component.addRetentionPolicy();
    component.onRetentionFrequencyChange(0, 'd');
    tick(400);

    expect(component.snapScheduleForm.get('retentionPolicies')?.invalid).toBe(true);
    discardPeriodicTasks();
  }));
});
