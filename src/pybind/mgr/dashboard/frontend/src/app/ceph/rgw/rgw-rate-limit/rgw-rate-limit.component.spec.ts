import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwRateLimitComponent } from './rgw-rate-limit.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { AbstractControl, ReactiveFormsModule, ValidationErrors } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { CheckboxModule, InputModule, NotificationService } from 'carbon-components-angular';
import { ToastrModule } from 'ngx-toastr';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

describe('RgwRateLimitComponent', () => {
  let component: RgwRateLimitComponent;
  let fixture: ComponentFixture<RgwRateLimitComponent>;
  configureTestBed({
    imports: [
      ReactiveFormsModule,
      ToastrModule.forRoot(),
      InputModule,
      CheckboxModule,
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot(),
      InputModule
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

  it('should call populateFormValues', () => {
    let data: any = {
      enabled: true,
      max_read_ops: 100,
      max_write_ops: 200,
      max_read_bytes: 10485760,
      max_write_bytes: 10485760
    };
    const ratelimitPropSpy = spyOn(component as any, '_setRateLimitProperty');
    component['populateFormValues'](data);
    expect(ratelimitPropSpy).toHaveBeenCalled();
  });

  describe('test case for _getRateLimitArgs', () => {
    it('should return expected result when all rate limits are specified', () => {
      // Using patchValue to set form values
      component.form.patchValue({
        rate_limit_enabled: true,
        rate_limit_max_readOps: 100,
        rate_limit_max_writeOps: 200,
        rate_limit_max_readBytes: '10Mb',
        rate_limit_max_writeBytes: '10Mb',
        rate_limit_max_readOps_unlimited: false,
        rate_limit_max_writeOps_unlimited: false,
        rate_limit_max_readBytes_unlimited: false,
        rate_limit_max_writeBytes_unlimited: false
      });
      jest.spyOn(FormatterService.prototype, 'toBytes').mockReturnValue(10485760); // 10MB in bytes

      const result = component['_getRateLimitArgs']();

      expect(result).toEqual({
        enabled: true,
        max_read_ops: 100,
        max_write_ops: 200,
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
        rate_limit_max_readBytes: '10MB',
        rate_limit_max_writeBytes: '20MB',
        rate_limit_max_readOps_unlimited: true, // Unlimited
        rate_limit_max_writeOps_unlimited: true, // Unlimited
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
