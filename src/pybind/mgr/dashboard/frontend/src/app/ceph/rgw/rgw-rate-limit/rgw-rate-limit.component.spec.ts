import { of } from 'rxjs';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { USER } from '~/app/shared/constants/app.constants';
import { RgwRateLimitComponent } from './rgw-rate-limit.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import {
  AbstractControl,
  FormsModule,
  ReactiveFormsModule,
  ValidationErrors
} from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { HelperComponent } from '~/app/shared/components/helper/helper.component';
import { CheckboxModule, InputModule, NotificationService } from 'carbon-components-angular';

import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

describe('RgwRateLimitComponent', () => {
  let component: RgwRateLimitComponent;
  let fixture: ComponentFixture<RgwRateLimitComponent>;
  configureTestBed({
    imports: [
      FormsModule,
      ReactiveFormsModule,
      InputModule,
      CheckboxModule,
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule
    ],
    declarations: [RgwRateLimitComponent],
    providers: [NotificationService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwRateLimitComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize list and delete ops controls as unlimited', () => {
    expect(component.form.get('rate_limit_max_listOps_unlimited').value).toBe(true);
    expect(component.form.get('rate_limit_max_listOps').value).toBeNull();
    expect(component.form.get('rate_limit_max_deleteOps_unlimited').value).toBe(true);
    expect(component.form.get('rate_limit_max_deleteOps').value).toBeNull();
  });

  it('should place list and delete ops after write ops and before byte limits', () => {
    expect(component.rateLimitFields.map((field) => field.apiKey)).toEqual([
      'max_read_ops',
      'max_write_ops',
      'max_list_ops',
      'max_delete_ops',
      'max_read_bytes',
      'max_write_bytes'
    ]);
  });

  it('should append list and delete thresholds to the global banner', () => {
    expect(component.globalBannerFields.map((field) => field.apiKey)).toEqual([
      'max_read_bytes',
      'max_read_ops',
      'max_write_bytes',
      'max_write_ops',
      'max_list_ops',
      'max_delete_ops'
    ]);
  });

  it('should describe dual throttling for list and delete operations', () => {
    const list = component.rateLimitFields.find((field) => field.apiKey === 'max_list_ops');
    const deleteOps = component.rateLimitFields.find((field) => field.apiKey === 'max_delete_ops');

    expect(list.helpText).toContain('maximum read ops');
    expect(list.unlimitedHelp).toContain('maximum read ops');
    expect(list.tooltip).toContain('maximum read ops');
    expect(deleteOps.helpText).toContain('maximum write ops');
    expect(deleteOps.helpText).toContain('S3 multi-object delete is not rate limited');
    expect(deleteOps.unlimitedHelp).toContain('S3 multi-object delete is not rate limited');
    expect(deleteOps.tooltip).toContain('S3 multi-object delete is not rate limited');
  });

  it('should return the standard validation messages for list and delete ops', () => {
    const list = component.rateLimitFields.find((field) => field.apiKey === 'max_list_ops');
    component.form.get('rate_limit_enabled').setValue(true);
    component.form.get(list.unlimitedControl).setValue(false);
    const control = component.form.get(list.control);
    control.setValue('');
    control.markAsDirty();
    expect(component.errorText(list)).toBe('This field is required.');

    const spy = jest
      .spyOn(FormatterService.prototype, 'iopmMaxSizeValidator')
      .mockReturnValue({ rateOpsMaxSize: true });
    control.setValue('abc');
    expect(component.errorText(list)).toBe('The value is not valid.');
    spy.mockRestore();
  });

  describe('rendered helper copy', () => {
    const render = (type: string) => {
      component.type = type;
      component.form.get('rate_limit_enabled').setValue(true);
      fixture.detectChanges();
      return fixture.nativeElement.textContent.replace(/\s+/g, ' ');
    };

    [USER, 'bucket'].forEach((type) => {
      it(`should explain list and delete limits on the ${type} form`, () => {
        const text = render(type);
        expect(text).toContain('List operations also count against the read operations limit');
        expect(text).toContain('S3 multi-object delete is not rate limited');
        expect(text).toContain('Unlimited list ops');
        expect(text).toContain('maximum read ops');
        expect(text).toContain('Unlimited delete ops');
        expect(text).toContain('maximum write ops');
      });
    });

    it('should attach list and delete rule tooltips', () => {
      render(USER);
      const tooltips = fixture.debugElement
        .queryAll(By.directive(HelperComponent))
        .map((helper) => helper.componentInstance.html);
      expect(tooltips).toEqual([
        component.rateLimitFields.find((field) => field.apiKey === 'max_list_ops').tooltip,
        component.rateLimitFields.find((field) => field.apiKey === 'max_delete_ops').tooltip
      ]);
    });

    it('should display active global list and delete thresholds', () => {
      component.type = USER;
      component.globalRateLimit = {
        enabled: true,
        max_read_ops: 1,
        max_write_ops: 2,
        max_list_ops: 40,
        max_delete_ops: 10,
        max_read_bytes: 0,
        max_write_bytes: 0
      };
      fixture.detectChanges();
      const text = fixture.nativeElement.textContent.replace(/\s+/g, ' ');
      expect(text).toContain('Max. list ops : 40');
      expect(text).toContain('Max. delete ops : 10');
    });
  });

  describe('list and delete ops validation', () => {
    ['rate_limit_max_listOps', 'rate_limit_max_deleteOps'].forEach((name) => {
      describe(name, () => {
        beforeEach(() => {
          component.form.get('rate_limit_enabled').setValue(true);
          component.form.get(`${name}_unlimited`).setValue(false);
        });

        it('should be required when enabled and not unlimited', () => {
          const control = component.form.get(name);
          control.setValue('');
          expect(control.hasError('required')).toBe(true);
        });

        it('should use the ops size validator', () => {
          const spy = jest
            .spyOn(FormatterService.prototype, 'iopmMaxSizeValidator')
            .mockReturnValue({ rateOpsMaxSize: true });
          const control = component.form.get(name);
          control.setValue('abc');
          expect(spy).toHaveBeenCalled();
          expect(control.hasError('rateOpsMaxSize')).toBe(true);
          spy.mockRestore();
        });

        it('should not be validated when unlimited', () => {
          component.form.get(`${name}_unlimited`).setValue(true);
          const control = component.form.get(name);
          control.setValue('');
          expect(control.valid).toBe(true);
        });

        it('should not be validated when rate limit is disabled', () => {
          component.form.get('rate_limit_enabled').setValue(false);
          const control = component.form.get(name);
          control.setValue('');
          expect(control.valid).toBe(true);
        });
      });
    });
  });

  it('should call populateFormValues', () => {
    let data: any = {
      enabled: true,
      max_read_ops: 100,
      max_write_ops: 200,
      max_list_ops: 40,
      max_delete_ops: 10,
      max_read_bytes: 10485760,
      max_write_bytes: 10485760
    };
    const ratelimitPropSpy = spyOn(component as any, '_setRateLimitProperty');
    component['populateFormValues'](data);
    expect(ratelimitPropSpy).toHaveBeenCalled();
    expect(ratelimitPropSpy).toHaveBeenCalledWith(
      'rate_limit_max_listOps',
      'rate_limit_max_listOps_unlimited',
      40
    );
    expect(ratelimitPropSpy).toHaveBeenCalledWith(
      'rate_limit_max_deleteOps',
      'rate_limit_max_deleteOps_unlimited',
      10
    );
  });

  it('should populate list and delete ops form values', () => {
    component['populateFormValues']({
      enabled: true,
      max_read_ops: 0,
      max_write_ops: 0,
      max_list_ops: 40,
      max_delete_ops: 10,
      max_read_bytes: 0,
      max_write_bytes: 0
    });
    expect(component.form.getValue('rate_limit_max_listOps_unlimited')).toBe(false);
    expect(component.form.getValue('rate_limit_max_listOps')).toBe(40);
    expect(component.form.getValue('rate_limit_max_deleteOps_unlimited')).toBe(false);
    expect(component.form.getValue('rate_limit_max_deleteOps')).toBe(10);
  });

  it('should mark list and delete ops as unlimited when populated with 0 or missing values', () => {
    component['populateFormValues']({
      enabled: true,
      max_read_ops: 0,
      max_write_ops: 0,
      max_list_ops: 0,
      max_read_bytes: 0,
      max_write_bytes: 0
    } as any);
    expect(component.form.getValue('rate_limit_max_listOps_unlimited')).toBe(true);
    expect(component.form.getValue('rate_limit_max_listOps')).toBe('');
    expect(component.form.getValue('rate_limit_max_deleteOps_unlimited')).toBe(true);
    expect(component.form.getValue('rate_limit_max_deleteOps')).toBe('');
  });

  describe('dirty state detection', () => {
    it('should return null when the form is pristine', () => {
      expect(component.getRateLimitFormValue()).toBeNull();
    });

    [
      'rate_limit_max_listOps_unlimited',
      'rate_limit_max_listOps',
      'rate_limit_max_deleteOps_unlimited',
      'rate_limit_max_deleteOps'
    ].forEach((name) => {
      it(`should detect ${name} as dirty`, () => {
        component.form.get(name).markAsDirty();
        expect(component['_isRateLimitFormDirty']()).toBe(true);
        expect(component.getRateLimitFormValue()).not.toBeNull();
      });
    });
  });

  describe('edit mode', () => {
    const limits = {
      enabled: true,
      max_read_ops: 100,
      max_write_ops: 200,
      max_list_ops: 40,
      max_delete_ops: 10,
      max_read_bytes: 0,
      max_write_bytes: 0
    };

    it('should load existing list and delete limits when editing a user', () => {
      const spy = jest
        .spyOn(TestBed.inject(RgwUserService), 'getUserRateLimit')
        .mockReturnValue(of({ user_ratelimit: limits }) as any);
      component.type = USER;
      component.isEditing = true;
      component.id = 'user1';
      component['getRateLimitFormValues']();

      expect(spy).toHaveBeenCalledWith('user1');
      expect(component.form.getValue('rate_limit_max_listOps_unlimited')).toBe(false);
      expect(component.form.getValue('rate_limit_max_listOps')).toBe(40);
      expect(component.form.getValue('rate_limit_max_deleteOps_unlimited')).toBe(false);
      expect(component.form.getValue('rate_limit_max_deleteOps')).toBe(10);
      // Loading existing values must not make the form dirty.
      expect(component.getRateLimitFormValue()).toBeNull();
    });

    it('should load existing list and delete limits when editing a bucket', () => {
      const spy = jest
        .spyOn(TestBed.inject(RgwBucketService), 'getBucketRateLimit')
        .mockReturnValue(of({ bucket_ratelimit: limits }) as any);
      component.type = 'bucket';
      component.isEditing = true;
      component.allowBid = 'bucket1';

      expect(spy).toHaveBeenCalledWith('bucket1');
      expect(component.form.getValue('rate_limit_max_listOps_unlimited')).toBe(false);
      expect(component.form.getValue('rate_limit_max_listOps')).toBe(40);
      expect(component.form.getValue('rate_limit_max_deleteOps_unlimited')).toBe(false);
      expect(component.form.getValue('rate_limit_max_deleteOps')).toBe(10);
      expect(component.getRateLimitFormValue()).toBeNull();
    });

    it('should send all loaded limits when only list ops is changed', () => {
      jest
        .spyOn(TestBed.inject(RgwUserService), 'getUserRateLimit')
        .mockReturnValue(of({ user_ratelimit: limits }) as any);
      jest.spyOn(FormatterService.prototype, 'toBytes').mockReturnValue(0);
      component.type = USER;
      component.id = 'user1';
      component['getRateLimitFormValues']();

      component.form.get('rate_limit_max_listOps').setValue(55);
      component.form.get('rate_limit_max_listOps').markAsDirty();

      expect(component.getRateLimitFormValue()).toEqual({
        enabled: true,
        max_read_ops: 100,
        max_write_ops: 200,
        max_list_ops: 55,
        max_delete_ops: 10,
        max_read_bytes: 0,
        max_write_bytes: 0
      });
    });

    it('should reset list and delete ops to 0 when set to unlimited while editing', () => {
      jest
        .spyOn(TestBed.inject(RgwUserService), 'getUserRateLimit')
        .mockReturnValue(of({ user_ratelimit: limits }) as any);
      jest.spyOn(FormatterService.prototype, 'toBytes').mockReturnValue(0);
      component.type = USER;
      component.id = 'user1';
      component['getRateLimitFormValues']();

      component.form.get('rate_limit_max_listOps_unlimited').setValue(true);
      component.form.get('rate_limit_max_listOps_unlimited').markAsDirty();
      component.form.get('rate_limit_max_deleteOps_unlimited').setValue(true);
      component.form.get('rate_limit_max_deleteOps_unlimited').markAsDirty();

      const result = component.getRateLimitFormValue();
      expect(result.max_list_ops).toBe(0);
      expect(result.max_delete_ops).toBe(0);
    });
  });

  describe('test case for _getRateLimitArgs', () => {
    it('should return expected result when all rate limits are specified', () => {
      // Using patchValue to set form values
      component.form.patchValue({
        rate_limit_enabled: true,
        rate_limit_max_readOps: 100,
        rate_limit_max_writeOps: 200,
        rate_limit_max_listOps: 40,
        rate_limit_max_deleteOps: 10,
        rate_limit_max_readBytes: '10Mb',
        rate_limit_max_writeBytes: '10Mb',
        rate_limit_max_readOps_unlimited: false,
        rate_limit_max_writeOps_unlimited: false,
        rate_limit_max_listOps_unlimited: false,
        rate_limit_max_deleteOps_unlimited: false,
        rate_limit_max_readBytes_unlimited: false,
        rate_limit_max_writeBytes_unlimited: false
      });
      jest.spyOn(FormatterService.prototype, 'toBytes').mockReturnValue(10485760); // 10MB in bytes

      const result = component['_getRateLimitArgs']();

      expect(result).toEqual({
        enabled: true,
        max_read_ops: 100,
        max_write_ops: 200,
        max_list_ops: 40,
        max_delete_ops: 10,
        max_read_bytes: 10485760, // Converted by FormatterService.toBytes
        max_write_bytes: 10485760 // Converted by FormatterService.toBytes
      });
    });
    it('should return default values for unlimited options', () => {
      // Using patchValue to set unlimited flags
      component.form.patchValue({
        rate_limit_enabled: true,
        rate_limit_max_readOps: 100,
        rate_limit_max_writeOps: 200,
        rate_limit_max_listOps: 40,
        rate_limit_max_deleteOps: 10,
        rate_limit_max_readBytes: '10MB',
        rate_limit_max_writeBytes: '20MB',
        rate_limit_max_readOps_unlimited: true, // Unlimited
        rate_limit_max_writeOps_unlimited: true, // Unlimited
        rate_limit_max_listOps_unlimited: true, // Unlimited
        rate_limit_max_deleteOps_unlimited: true, // Unlimited
        rate_limit_max_readBytes_unlimited: true, // Unlimited
        rate_limit_max_writeBytes_unlimited: true // Unlimited
      });

      // Set return values for the spied methods (not used for unlimited cases)
      jest.spyOn(FormatterService.prototype, 'toBytes').mockReturnValue(10485760); // 10MB in bytes

      const result = component['_getRateLimitArgs']();

      expect(result).toEqual({
        enabled: true,
        max_read_ops: 0, // Default value when unlimited
        max_write_ops: 0, // Default value when unlimited
        max_list_ops: 0, // Default value when unlimited
        max_delete_ops: 0, // Default value when unlimited
        max_read_bytes: 0, // Default value when unlimited
        max_write_bytes: 0 // Default value when unlimited
      });
    });
  });

  it('should call _setRateLimitProperty when value is equal to 0 ', () => {
    const mockrateLimitKey = 'rate_limit_max_readBytes';
    const mockunlimitedKey = 'rate_limit_max_readBytes_unlimited';
    const mockproperty = 0;

    component['_setRateLimitProperty'](mockrateLimitKey, mockunlimitedKey, mockproperty);
    expect(component.form.getValue('rate_limit_max_readBytes_unlimited')).toEqual(true);
    expect(component.form.getValue('rate_limit_max_readBytes')).toEqual('');
  });
  it('should call rateLimitIopmMaxSizeValidator and return result', () => {
    const mockResult: ValidationErrors = { someError: true };
    FormatterService.prototype.iopmMaxSizeValidator = jest.fn().mockReturnValue(mockResult);
    const control: AbstractControl = { value: 'testValue' } as AbstractControl;
    const result = component.rateLimitIopmMaxSizeValidator(control);
    expect(FormatterService.prototype.iopmMaxSizeValidator).toHaveBeenCalledWith(control);
    expect(result).toEqual(mockResult);
  });
  it('should call rateLimitIopmMaxSizeValidator and return null', () => {
    FormatterService.prototype.iopmMaxSizeValidator = jest.fn().mockReturnValue(null);
    const control: AbstractControl = { value: 'testValue' } as AbstractControl;
    const result = component.rateLimitIopmMaxSizeValidator(control);
    expect(FormatterService.prototype.iopmMaxSizeValidator).toHaveBeenCalledWith(control);
    expect(result).toBeNull();
  });
  it('should call the rateLimitBytesMaxSizeValidator and FormatterService method with the correct parameters', () => {
    const control: AbstractControl = {
      value: '1000 K'
    } as AbstractControl;

    const serviceSpy = jest
      .spyOn(FormatterService.prototype, 'performValidation')
      .mockReturnValue(null);
    const result = component.rateLimitBytesMaxSizeValidator(control);
    expect(serviceSpy).toHaveBeenCalledWith(
      control,
      '^(\\d+(\\.\\d+)?)\\s*(B/m|K(B|iB/m)?|M(B|iB/m)?|G(B|iB/m)?|T(B|iB/m)?|P(B|iB/m)?)?$',
      { rateByteMaxSize: true }
    );
    expect(result).toBeNull();
  });
});
