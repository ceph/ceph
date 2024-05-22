import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSnapshotscheduleFormComponent } from './cephfs-snapshotschedule-form.component';
import {
  NgbActiveModal,
  NgbDatepickerModule,
  NgbTimepickerModule
} from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { FormHelper, configureTestBed } from '~/testing/unit-test-helper';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';

describe('CephfsSnapshotscheduleFormComponent', () => {
  let component: CephfsSnapshotscheduleFormComponent;
  let fixture: ComponentFixture<CephfsSnapshotscheduleFormComponent>;
  let formHelper: FormHelper;
  let createSpy: jasmine.Spy;

  configureTestBed({
    declarations: [CephfsSnapshotscheduleFormComponent],
    providers: [NgbActiveModal],
    imports: [
      SharedModule,
      ToastrModule.forRoot(),
      ReactiveFormsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      NgbDatepickerModule,
      NgbTimepickerModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSnapshotscheduleFormComponent);
    component = fixture.componentInstance;
    component.fsName = 'test_fs';
    component.ngOnInit();
    formHelper = new FormHelper(component.snapScheduleForm);
    createSpy = spyOn(TestBed.inject(CephfsSnapshotScheduleService), 'create').and.stub();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a form open in modal', () => {
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cd-modal')).not.toBe(null);
  });

  it('should submit the form', () => {
    const input = {
      directory: '/test',
      startDate: {
        year: 2023,
        month: 11,
        day: 14
      },
      startTime: {
        hour: 0,
        minute: 6,
        second: 22
      },
      repeatInterval: 4,
      repeatFrequency: 'h'
    };

    formHelper.setMultipleValues(input);
    component.snapScheduleForm.get('directory').setValue('/test');
    component.submit();

    expect(createSpy).toHaveBeenCalled();
  });
});
