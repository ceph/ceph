import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiSettingComponent } from '../iscsi-setting/iscsi-setting.component';
import { IscsiTargetIqnSettingsModalComponent } from './iscsi-target-iqn-settings-modal.component';

describe('IscsiTargetIqnSettingsModalComponent', () => {
  let component: IscsiTargetIqnSettingsModalComponent;
  let fixture: ComponentFixture<IscsiTargetIqnSettingsModalComponent>;

  configureTestBed({
    declarations: [IscsiTargetIqnSettingsModalComponent, IscsiSettingComponent],
    imports: [SharedModule, ReactiveFormsModule, HttpClientTestingModule, RouterTestingModule],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetIqnSettingsModalComponent);
    component = fixture.componentInstance;
    component.target_controls = new FormControl({});
    component.target_default_controls = {
      cmdsn_depth: 1,
      dataout_timeout: 2,
      first_burst_length: true
    };
    component.target_controls_limits = {
      cmdsn_depth: {
        min: 1,
        max: 512,
        type: 'int'
      },
      dataout_timeout: {
        min: 2,
        max: 60,
        type: 'int'
      },
      first_burst_length: {
        max: 16777215,
        min: 512,
        type: 'int'
      }
    };
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should fill the settingsForm', () => {
    expect(component.settingsForm.value).toEqual({
      cmdsn_depth: null,
      dataout_timeout: null,
      first_burst_length: null
    });
  });

  it('should save changes to target_controls', () => {
    component.settingsForm.patchValue({ dataout_timeout: 1234 });
    expect(component.target_controls.value).toEqual({});
    component.save();
    expect(component.target_controls.value).toEqual({ dataout_timeout: 1234 });
  });
});
