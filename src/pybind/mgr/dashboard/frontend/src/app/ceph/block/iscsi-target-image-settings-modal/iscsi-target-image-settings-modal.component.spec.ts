import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetImageSettingsModalComponent } from './iscsi-target-image-settings-modal.component';

describe('IscsiTargetImageSettingsModalComponent', () => {
  let component: IscsiTargetImageSettingsModalComponent;
  let fixture: ComponentFixture<IscsiTargetImageSettingsModalComponent>;

  configureTestBed({
    declarations: [IscsiTargetImageSettingsModalComponent],
    imports: [SharedModule, FormsModule, HttpClientTestingModule, RouterTestingModule],
    providers: [BsModalRef, i18nProviders]
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
    component.backstores = ['backstore:1', 'backstore:2'];

    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should fill the model', () => {
    expect(component.model).toEqual({
      backstore: 'backstore:1',
      'backstore:1': {},
      'backstore:2': {}
    });
  });

  it('should save changes to imagesSettings', () => {
    component.model['backstore:1'] = { foo: 1234 };
    expect(component.imagesSettings).toEqual({
      'rbd/disk_1': { backstore: 'backstore:1', 'backstore:1': {} }
    });
    component.save();
    expect(component.imagesSettings).toEqual({
      'rbd/disk_1': {
        backstore: 'backstore:1',
        'backstore:1': {
          foo: 1234
        }
      }
    });
  });
});
