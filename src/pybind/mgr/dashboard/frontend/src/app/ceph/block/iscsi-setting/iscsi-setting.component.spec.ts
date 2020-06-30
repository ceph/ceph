import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, NgForm, ReactiveFormsModule } from '@angular/forms';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiSettingComponent } from './iscsi-setting.component';

describe('IscsiSettingComponent', () => {
  let component: IscsiSettingComponent;
  let fixture: ComponentFixture<IscsiSettingComponent>;

  configureTestBed({
    imports: [SharedModule, ReactiveFormsModule],
    declarations: [IscsiSettingComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiSettingComponent);
    component = fixture.componentInstance;
    component.settingsForm = new CdFormGroup({
      max_data_area_mb: new FormControl()
    });
    component.formDir = new NgForm([], []);
    component.setting = 'max_data_area_mb';
    component.limits = {
      type: 'int',
      min: 1,
      max: 2048
    };
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
