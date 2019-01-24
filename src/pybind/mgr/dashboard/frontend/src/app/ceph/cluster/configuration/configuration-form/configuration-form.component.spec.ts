import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { ConfigurationFormComponent } from './configuration-form.component';
import { ConfigFormModel } from './configuration-form.model';

describe('ConfigurationFormComponent', () => {
  let component: ConfigurationFormComponent;
  let fixture: ComponentFixture<ConfigurationFormComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      ToastModule.forRoot(),
      SharedModule
    ],
    declarations: [ConfigurationFormComponent],
    providers: [
      {
        provide: ActivatedRoute
      },
      i18nProviders
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

  describe('getStep', () => {
    it('should return the correct step for type uint and value 0', () => {
      const ret = component.getStep('uint', 0);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type int and value 1', () => {
      const ret = component.getStep('int', 1);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type int and value null', () => {
      const ret = component.getStep('int', null);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type size and value 2', () => {
      const ret = component.getStep('size', 2);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type secs and value 3', () => {
      const ret = component.getStep('secs', 3);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type float and value 1', () => {
      const ret = component.getStep('float', 1);
      expect(ret).toBe(0.1);
    });

    it('should return the correct step for type float and value 0.1', () => {
      const ret = component.getStep('float', 0.1);
      expect(ret).toBe(0.1);
    });

    it('should return the correct step for type float and value 0.02', () => {
      const ret = component.getStep('float', 0.02);
      expect(ret).toBe(0.01);
    });

    it('should return the correct step for type float and value 0.003', () => {
      const ret = component.getStep('float', 0.003);
      expect(ret).toBe(0.001);
    });

    it('should return the correct step for type float and value null', () => {
      const ret = component.getStep('float', null);
      expect(ret).toBe(0.1);
    });

    it('should return undefined for unknown type', () => {
      const ret = component.getStep('unknown', 1);
      expect(ret).toBeUndefined();
    });
  });
});
