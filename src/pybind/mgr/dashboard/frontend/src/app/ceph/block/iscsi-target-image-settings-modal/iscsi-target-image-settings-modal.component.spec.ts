import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiSettingComponent } from '../iscsi-setting/iscsi-setting.component';
import { IscsiTargetImageSettingsModalComponent } from './iscsi-target-image-settings-modal.component';

describe('IscsiTargetImageSettingsModalComponent', () => {
  let component: IscsiTargetImageSettingsModalComponent;
  let fixture: ComponentFixture<IscsiTargetImageSettingsModalComponent>;

  configureTestBed({
    declarations: [IscsiTargetImageSettingsModalComponent, IscsiSettingComponent],
    imports: [SharedModule, ReactiveFormsModule, HttpClientTestingModule, RouterTestingModule],
    providers: [NgbActiveModal, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetImageSettingsModalComponent);
    component = fixture.componentInstance;

    component.imagesSettings = { 'rbd/disk_1': { backstore: 'backstore:1', 'backstore:1': {} } };
    component.image = 'rbd/disk_1';
    component.disk_default_controls = {
      'backstore:1': {
        foo: 1,
        bar: 2
      },
      'backstore:2': {
        baz: 3
      }
    };
    component.disk_controls_limits = {
      'backstore:1': {
        foo: {
          min: 1,
          max: 512,
          type: 'int'
        },
        bar: {
          min: 1,
          max: 512,
          type: 'int'
        }
      },
      'backstore:2': {
        baz: {
          min: 1,
          max: 512,
          type: 'int'
        }
      }
    };
    component.backstores = ['backstore:1', 'backstore:2'];
    component.control = new FormControl();

    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should fill the form', () => {
    expect(component.settingsForm.value).toEqual({
      lun: null,
      wwn: null,
      backstore: 'backstore:1',
      foo: null,
      bar: null,
      baz: null
    });
  });

  it('should save changes to imagesSettings', () => {
    component.settingsForm.controls['foo'].setValue(1234);
    expect(component.imagesSettings).toEqual({
      'rbd/disk_1': { backstore: 'backstore:1', 'backstore:1': {} }
    });
    component.save();
    expect(component.imagesSettings).toEqual({
      'rbd/disk_1': {
        lun: null,
        wwn: null,
        backstore: 'backstore:1',
        'backstore:1': {
          foo: 1234
        }
      }
    });
  });
});
