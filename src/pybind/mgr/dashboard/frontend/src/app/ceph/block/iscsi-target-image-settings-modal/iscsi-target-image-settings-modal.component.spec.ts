import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetImageSettingsModalComponent } from './iscsi-target-image-settings-modal.component';

describe('IscsiTargetImageSettingsModalComponent', () => {
  let component: IscsiTargetImageSettingsModalComponent;
  let fixture: ComponentFixture<IscsiTargetImageSettingsModalComponent>;

  configureTestBed({
    declarations: [IscsiTargetImageSettingsModalComponent],
    imports: [SharedModule, ReactiveFormsModule, HttpClientTestingModule],
    providers: [BsModalRef, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetImageSettingsModalComponent);
    component = fixture.componentInstance;

    component.imagesSettings = { 'rbd/disk_1': {} };
    component.image = 'rbd/disk_1';
    component.disk_default_controls = {
      foo: 1,
      bar: 2
    };
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should fill the settingsForm', () => {
    expect(component.settingsForm.value).toEqual({
      foo: null,
      bar: null
    });
  });

  it('should save changes to imagesSettings', () => {
    component.settingsForm.patchValue({ foo: 1234 });
    expect(component.imagesSettings).toEqual({ 'rbd/disk_1': {} });
    component.save();
    expect(component.imagesSettings).toEqual({
      'rbd/disk_1': {
        foo: 1234
      }
    });
  });
});
