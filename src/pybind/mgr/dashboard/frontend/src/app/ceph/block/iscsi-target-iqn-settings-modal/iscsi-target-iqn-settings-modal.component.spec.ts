import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetIqnSettingsModalComponent } from './iscsi-target-iqn-settings-modal.component';

describe('IscsiTargetIqnSettingsModalComponent', () => {
  let component: IscsiTargetIqnSettingsModalComponent;
  let fixture: ComponentFixture<IscsiTargetIqnSettingsModalComponent>;

  configureTestBed({
    declarations: [IscsiTargetIqnSettingsModalComponent],
    imports: [SharedModule, ReactiveFormsModule, HttpClientTestingModule, RouterTestingModule],
    providers: [BsModalRef, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetIqnSettingsModalComponent);
    component = fixture.componentInstance;
    component.target_controls = new FormControl({});
    component.target_default_controls = {
      cmdsn_depth: 1,
      dataout_timeout: 2,
      first_burst_length: 'Yes'
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
