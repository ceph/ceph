import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { ConfigFormModel } from '../../../../shared/components/config-option/config-option.model';
import { SharedModule } from '../../../../shared/shared.module';
import { ConfigurationFormComponent } from './configuration-form.component';

describe('ConfigurationFormComponent', () => {
  let component: ConfigurationFormComponent;
  let fixture: ComponentFixture<ConfigurationFormComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      SharedModule
    ],
    declarations: [ConfigurationFormComponent],
    providers: [
      {
        provide: ActivatedRoute
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigurationFormComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('getValidators', () => {
    it('should return a validator for types float, addr and uuid', () => {
      const types = ['float', 'addr', 'uuid'];

      types.forEach((valType) => {
        const configOption = new ConfigFormModel();
        configOption.type = valType;

        const ret = component.getValidators(configOption);
        expect(ret).toBeTruthy();
        expect(ret.length).toBe(1);
      });
    });

    it('should not return a validator for types str and bool', () => {
      const types = ['str', 'bool'];

      types.forEach((valType) => {
        const configOption = new ConfigFormModel();
        configOption.type = valType;

        const ret = component.getValidators(configOption);
        expect(ret).toBeUndefined();
      });
    });

    it('should return a pattern and a min validator', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'int';
      configOption.min = 2;

      const ret = component.getValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.length).toBe(2);
      expect(component.minValue).toBe(2);
      expect(component.maxValue).toBeUndefined();
    });

    it('should return a pattern and a max validator', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'int';
      configOption.max = 5;

      const ret = component.getValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.length).toBe(2);
      expect(component.minValue).toBeUndefined();
      expect(component.maxValue).toBe(5);
    });

    it('should return multiple validators', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'float';
      configOption.max = 5.2;
      configOption.min = 1.5;

      const ret = component.getValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.length).toBe(3);
      expect(component.minValue).toBe(1.5);
      expect(component.maxValue).toBe(5.2);
    });
  });
});
