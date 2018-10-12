import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { ConfigurationFormComponent } from './configuration-form.component';
import { ConfigFormModel } from './configuration-form.model';

describe('ConfigurationFormComponent', () => {
  let component: ConfigurationFormComponent;
  let fixture: ComponentFixture<ConfigurationFormComponent>;
  let activatedRoute: ActivatedRoute;

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
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigurationFormComponent);
    component = fixture.componentInstance;
    activatedRoute = TestBed.get(ActivatedRoute);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('getType', () => {
    it('should return uint64_t type', () => {
      const ret = component.getType('uint64_t');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('uint64_t');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Positive integer value');
      expect(ret.defaultMin).toBe(0);
      expect(ret.patternHelpText).toBe('The entered value needs to be a positive number.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(false);
    });

    it('should return int64_t type', () => {
      const ret = component.getType('int64_t');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('int64_t');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Integer value');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBe('The entered value needs to be a number.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(true);
    });

    it('should return size_t type', () => {
      const ret = component.getType('size_t');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('size_t');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Positive integer value (size)');
      expect(ret.defaultMin).toBe(0);
      expect(ret.patternHelpText).toBe('The entered value needs to be a positive number.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(false);
    });

    it('should return secs type', () => {
      const ret = component.getType('secs');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('secs');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Positive integer value (secs)');
      expect(ret.defaultMin).toBe(1);
      expect(ret.patternHelpText).toBe('The entered value needs to be a positive number.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(false);
    });

    it('should return double type', () => {
      const ret = component.getType('double');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('double');
      expect(ret.inputType).toBe('number');
      expect(ret.humanReadable).toBe('Decimal value');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBe('The entered value needs to be a number or decimal.');
      expect(ret.isNumberType).toBe(true);
      expect(ret.allowsNegative).toBe(true);
    });

    it('should return std::string type', () => {
      const ret = component.getType('std::string');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('std::string');
      expect(ret.inputType).toBe('text');
      expect(ret.humanReadable).toBe('Text');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBeUndefined();
      expect(ret.isNumberType).toBe(false);
      expect(ret.allowsNegative).toBeUndefined();
    });

    it('should return entity_addr_t type', () => {
      const ret = component.getType('entity_addr_t');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('entity_addr_t');
      expect(ret.inputType).toBe('text');
      expect(ret.humanReadable).toBe('IPv4 or IPv6 address');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBe('The entered value needs to be a valid IP address.');
      expect(ret.isNumberType).toBe(false);
      expect(ret.allowsNegative).toBeUndefined();
    });

    it('should return uuid_d type', () => {
      const ret = component.getType('uuid_d');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('uuid_d');
      expect(ret.inputType).toBe('text');
      expect(ret.humanReadable).toBe('UUID');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBe(
        'The entered value is not a valid UUID, e.g.: 67dcac9f-2c03-4d6c-b7bd-1210b3a259a8'
      );
      expect(ret.isNumberType).toBe(false);
      expect(ret.allowsNegative).toBeUndefined();
    });

    it('should return bool type', () => {
      const ret = component.getType('bool');
      expect(ret).toBeTruthy();
      expect(ret.name).toBe('bool');
      expect(ret.inputType).toBe('checkbox');
      expect(ret.humanReadable).toBe('Boolean value');
      expect(ret.defaultMin).toBeUndefined();
      expect(ret.patternHelpText).toBeUndefined();
      expect(ret.isNumberType).toBe(false);
      expect(ret.allowsNegative).toBeUndefined();
    });

    it('should throw an error for unknown type', () => {
      expect(() =>
        component.getType('unknown').toThrowError('Found unknown type "unknown" for config option.')
      );
    });
  });

  describe('getValidators', () => {
    it('should return a validator for types double, entity_addr_t and uuid_d', () => {
      const types = ['double', 'entity_addr_t', 'uuid_d'];

      types.forEach((valType) => {
        const configOption = new ConfigFormModel();
        configOption.type = valType;

        const ret = component.getValidators(configOption);
        expect(ret).toBeTruthy();
        expect(ret.length).toBe(1);
      });
    });

    it('should not return a validator for types std::string and bool', () => {
      const types = ['std::string', 'bool'];

      types.forEach((valType) => {
        const configOption = new ConfigFormModel();
        configOption.type = valType;

        const ret = component.getValidators(configOption);
        expect(ret).toBeUndefined();
      });
    });

    it('should return a pattern and a min validator', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'int64_t';
      configOption.min = 2;

      const ret = component.getValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.length).toBe(2);
      expect(component.minValue).toBe(2);
      expect(component.maxValue).toBeUndefined();
    });

    it('should return a pattern and a max validator', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'int64_t';
      configOption.max = 5;

      const ret = component.getValidators(configOption);
      expect(ret).toBeTruthy();
      expect(ret.length).toBe(2);
      expect(component.minValue).toBeUndefined();
      expect(component.maxValue).toBe(5);
    });

    it('should return multiple validators', () => {
      const configOption = new ConfigFormModel();
      configOption.type = 'double';
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
    it('should return the correct step for type uint64_t and value 0', () => {
      const ret = component.getStep('uint64_t', 0);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type int64_t and value 1', () => {
      const ret = component.getStep('int64_t', 1);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type int64_t and value null', () => {
      const ret = component.getStep('int64_t', null);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type size_t and value 2', () => {
      const ret = component.getStep('size_t', 2);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type secs and value 3', () => {
      const ret = component.getStep('secs', 3);
      expect(ret).toBe(1);
    });

    it('should return the correct step for type double and value 1', () => {
      const ret = component.getStep('double', 1);
      expect(ret).toBe(0.1);
    });

    it('should return the correct step for type double and value 0.1', () => {
      const ret = component.getStep('double', 0.1);
      expect(ret).toBe(0.1);
    });

    it('should return the correct step for type double and value 0.02', () => {
      const ret = component.getStep('double', 0.02);
      expect(ret).toBe(0.01);
    });

    it('should return the correct step for type double and value 0.003', () => {
      const ret = component.getStep('double', 0.003);
      expect(ret).toBe(0.001);
    });

    it('should return the correct step for type double and value null', () => {
      const ret = component.getStep('double', null);
      expect(ret).toBe(0.1);
    });

    it('should return undefined for unknown type', () => {
      const ret = component.getStep('unknown', 1);
      expect(ret).toBeUndefined();
    });
  });
});
