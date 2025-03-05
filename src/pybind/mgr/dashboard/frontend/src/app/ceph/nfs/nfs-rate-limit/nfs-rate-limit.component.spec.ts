import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NfsRateLimitComponent } from './nfs-rate-limit.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { Validators } from '@angular/forms';
import { FormatterService } from '~/app/shared/services/formatter.service';

describe('NfsRateLimitComponent', () => {
  let component: NfsRateLimitComponent;
  let fixture: ComponentFixture<NfsRateLimitComponent>;

  configureTestBed({
    declarations: [NfsRateLimitComponent],
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
    providers: [FormatterService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsRateLimitComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('loadConditionCheck', () => {
    beforeEach(() => {
      component.isEdit = true;
    });

    it('should handle type "cluster" with QoS enabled', () => {
      component.type = 'cluster';
      component.nfsClusterData = { enable_qos: true } as any;
      spyOn(component, 'setFormData');

      component.loadConditionCheck();

      expect(component.showDisableWarning).not.toBeTruthy();
      expect(component.allowQoS).toBeTruthy();
      expect(component.clusterQosDisabled).not.toBeTruthy();
      expect(component.setFormData).toHaveBeenCalledWith(component.nfsClusterData);
    });

    it('should handle type "export" with cluster QoS enabled', () => {
      component.type = 'export';
      component.nfsClusterData = { enable_qos: true } as any;
      spyOn(component, 'setFormData');

      component.loadConditionCheck();

      expect(component.showDisableWarning).not.toBeTruthy();
      expect(component.allowQoS).toBeTruthy();
      expect(component.setFormData).toHaveBeenCalledWith(component.nfsExportdata);
    });

    it('should handle type "export" with export QoS enabled and cluster QoS disabled', () => {
      component.type = 'export';
      component.nfsClusterData = { enable_qos: false } as any;
      component.nfsExportdata = { enable_qos: true } as any;
      spyOn(component, 'registerQoSChange');
      spyOn(component, 'setFormData');
      spyOn(component, 'showDisabledField');

      component.loadConditionCheck();

      expect(component.showDisableWarning).not.toBeTruthy();
      expect(component.allowQoS).toBeTruthy();
      expect(component.registerQoSChange).toHaveBeenCalledWith(component.nfsClusterData.qos_type);
      expect(component.setFormData).toHaveBeenCalledWith(component.nfsExportdata);
      expect(component.showDisabledField).toHaveBeenCalled();
    });

    it('should handle type "export" with both cluster and export QoS disabled', () => {
      component.type = 'export';
      component.nfsClusterData = { enable_qos: false } as any;
      component.nfsExportdata = { enable_qos: false } as any;

      component.loadConditionCheck();

      expect(component.showDisableWarning).not.toBeTruthy();
      expect(component.allowQoS).not.toBeTruthy();
    });
  });
  describe('setValidation', () => {
    it('should set validators for PerShare QoS type', () => {
      component.qosTypeVal = 'PerShare';
      component.createForm();
      jest.spyOn(component.rateLimitForm.controls['max_export_read_bw'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_export_write_bw'], 'addValidators');

      component.setValidation();

      expect(
        component.rateLimitForm.controls['max_export_read_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_export_write_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
    });

    it('should set validators for PerClient QoS type', () => {
      component.qosTypeVal = 'PerClient';
      component.createForm();
      jest.spyOn(component.rateLimitForm.controls['max_client_read_bw'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_write_bw'], 'addValidators');

      component.setValidation();

      expect(
        component.rateLimitForm.controls['max_client_read_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_write_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
    });

    it('should set validators for PerSharePerClient QoS type', () => {
      component.qosTypeVal = 'PerShare_PerClient';
      component.createForm();
      jest.spyOn(component.rateLimitForm.controls['max_export_read_bw'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_export_write_bw'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_read_bw'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_write_bw'], 'addValidators');

      component.setValidation();

      expect(
        component.rateLimitForm.controls['max_export_read_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_export_write_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_read_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_write_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
    });
  });

  describe('showMaxBwNote', () => {
    beforeEach(() => {
      component.createForm();
    });

    it('should add validators when QoS is enabled', () => {
      jest.spyOn(component, 'setValidation');
      component.showMaxBwNote(true);
      expect(component.setValidation).toHaveBeenCalled();
    });

    it('should remove validators when QoS is disabled', () => {
      jest.spyOn(component.rateLimitForm.controls['max_export_read_bw'], 'removeValidators');
      jest.spyOn(component.rateLimitForm.controls['max_export_write_bw'], 'removeValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_read_bw'], 'removeValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_write_bw'], 'removeValidators');

      component.showMaxBwNote(false);

      expect(
        component.rateLimitForm.controls['max_export_read_bw'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_export_write_bw'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_read_bw'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_write_bw'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
    });
  });

  describe('registerQoSChange', () => {
    it('should set validation and show note if QoS type is changed', () => {
      component.isEdit = true;
      component.nfsClusterData = { qos_type: 'oldType' } as any;
      jest.spyOn(component, 'setValidation');

      component.registerQoSChange('newType');

      expect(component.setValidation).toHaveBeenCalled();
      expect(component.showQostypeChangeNote).toBeTruthy();
    });

    it('should set validation and not show note if QoS type is not changed', () => {
      component.isEdit = true;
      component.nfsClusterData = { qos_type: 'sameType' } as any;
      jest.spyOn(component, 'setValidation');

      component.registerQoSChange('sameType');

      expect(component.setValidation).toHaveBeenCalled();
      expect(component.showQostypeChangeNote).toBeFalsy();
    });
  });

  describe('showQOS', () => {
    it('should allow QoS if type is export and cluster QoS is disabled', () => {
      component.type = 'export';
      component.nfsClusterData = { enable_qos: false } as any;

      component.showQOS();

      expect(component.allowQoS).toBeTruthy();
    });

    it('should not allow QoS if type is not export', () => {
      component.type = 'cluster';
      component.nfsClusterData = { enable_qos: false } as any;

      component.showQOS();

      expect(component.allowQoS).toBeFalsy();
    });
  });

  describe('setFormData', () => {
    beforeEach(() => {
      component.createForm();
    });

    it('should set form data correctly', () => {
      const data = {
        enable_qos: true,
        qos_type: 'PerShare',
        max_export_read_bw: 100,
        max_export_write_bw: 200,
        max_client_read_bw: 300,
        max_client_write_bw: 400
      };
      jest.spyOn(FormatterService.prototype, 'toBytes').mockReturnValue(100);

      component.setFormData(data);

      expect(component.rateLimitForm.get('enable_qos')?.value).toBe(data.enable_qos);
      expect(component.rateLimitForm.get('qos_type')?.value).toBe(data.qos_type);
      expect(component.rateLimitForm.get('max_export_read_bw')?.value).toBe(100);
      expect(component.rateLimitForm.get('max_export_write_bw')?.value).toBe(100);
      expect(component.rateLimitForm.get('max_client_read_bw')?.value).toBe(100);
      expect(component.rateLimitForm.get('max_client_write_bw')?.value).toBe(100);
    });
  });

  describe('getRateLimitFormValue', () => {
    it('should return rate limit args if form is dirty', () => {
      jest.spyOn(component, '_isRateLimitFormDirty').mockReturnValue(true);
      const rateLimitArgs = { enable_qos: true } as any;
      jest.spyOn(component, '_getRateLimitArgs').mockReturnValue(rateLimitArgs);

      const result = component.getRateLimitFormValue();

      expect(result).toBe(rateLimitArgs);
    });

    it('should return null if form is not dirty', () => {
      jest.spyOn(component, '_isRateLimitFormDirty').mockReturnValue(false);

      const result = component.getRateLimitFormValue();

      expect(result).toBeNull();
    });
  });

  describe('rateLimitBytesMaxSizeValidator', () => {
    it('should return validation error for invalid input', () => {
      const control = { value: 'invalid' } as any;
      jest
        .spyOn(FormatterService.prototype, 'performValidation')
        .mockReturnValue({ rateByteMaxSize: true });

      const result = component.rateLimitBytesMaxSizeValidator(control);

      expect(result).toEqual({ rateByteMaxSize: true });
    });

    it('should return null for valid input', () => {
      const control = { value: '100MB/s' } as any;
      jest.spyOn(FormatterService.prototype, 'performValidation').mockReturnValue(null);

      const result = component.rateLimitBytesMaxSizeValidator(control);

      expect(result).toBeNull();
    });
  });
  describe('createForm', () => {
    it('should create the form with default values', () => {
      component.createForm();

      expect(component.rateLimitForm.get('enable_qos')?.value).toBe(false);
      expect(component.rateLimitForm.get('qos_type')?.value).toBe('');
      expect(component.rateLimitForm.get('bwType')?.value).toBe('Individual');
      expect(component.rateLimitForm.get('max_export_read_bw')?.value).toBeNull();
      expect(component.rateLimitForm.get('max_export_write_bw')?.value).toBeNull();
      expect(component.rateLimitForm.get('max_client_read_bw')?.value).toBeNull();
      expect(component.rateLimitForm.get('max_client_write_bw')?.value).toBeNull();
    });
  });

  describe('getbwTypeHelp', () => {
    it('should return the help text for the given bwType', () => {
      component.bwTypeArr = [{ value: 'Individual', help: 'Individual help text' }];
      const result = component.getbwTypeHelp('Individual');
      expect(result).toBe('Individual help text');
    });

    it('should return an empty string if bwType is not found', () => {
      component.bwTypeArr = [{ value: 'Combined', help: 'Combined help text' }];
      const result = component.getbwTypeHelp('Individual');
      expect(result).toBe('');
    });
  });

  describe('getQoSTypeHelper', () => {
    it('should return the help text for the given qosType', () => {
      component.qosType = [{ value: 'PerShare', help: 'PerShare help text', key: 'PerShare' }];
      const result = component.getQoSTypeHelper('PerShare');
      expect(result).toBe('PerShare help text');
    });

    it('should return an empty string if qosType is not found', () => {
      component.qosType = [{ value: 'PerClient', help: 'PerClient help text', key: 'PerClient' }];
      const result = component.getQoSTypeHelper('PerShare');
      expect(result).toBe('');
    });
  });

  describe('showDisabledField', () => {
    beforeEach(() => {
      component.createForm();
    });

    it('should disable the appropriate fields', () => {
      component.showDisabledField();

      expect(component.rateLimitForm.get('enable_qos')?.disabled).toBeTruthy();
      expect(component.rateLimitForm.get('max_export_read_bw')?.disabled).toBeTruthy();
      expect(component.rateLimitForm.get('max_export_write_bw')?.disabled).toBeTruthy();
      expect(component.rateLimitForm.get('max_client_read_bw')?.disabled).toBeTruthy();
      expect(component.rateLimitForm.get('max_client_write_bw')?.disabled).toBeTruthy();
    });
  });

  describe('_isRateLimitFormDirty', () => {
    beforeEach(() => {
      component.createForm();
    });

    it('should return true if any form control is dirty', () => {
      component.rateLimitForm.get('enable_qos')?.markAsDirty();
      const result = component._isRateLimitFormDirty();
      expect(result).toBeTruthy();
    });

    it('should return false if no form control is dirty', () => {
      const result = component._isRateLimitFormDirty();
      expect(result).toBeFalsy();
    });
  });

  describe('_getRateLimitArgs', () => {
    beforeEach(() => {
      component.createForm();
      component.qosTypeVal = 'PerShare';
    });

    it('should return the correct rate limit args for PerShare', () => {
      component.rateLimitForm.get('enable_qos')?.setValue(true);
      component.rateLimitForm.get('qos_type')?.setValue('PerShare');
      component.rateLimitForm.get('max_export_read_bw')?.setValue('100MB/s');
      component.rateLimitForm.get('max_export_write_bw')?.setValue('200MB/s');
      jest.spyOn(FormatterService.prototype, 'toBytes').mockReturnValue(100);

      const result = component._getRateLimitArgs();

      expect(result.enable_qos).toBe(true);
      expect(result.qos_type).toBe('PerShare');
      expect(result.max_export_read_bw).toBe(100);
      expect(result.max_export_write_bw).toBe(100);
    });

    it('should return the correct rate limit args for PerClient', () => {
      component.qosTypeVal = 'PerClient';
      component.rateLimitForm.get('enable_qos')?.setValue(true);
      component.rateLimitForm.get('qos_type')?.setValue('PerClient');
      component.rateLimitForm.get('max_client_read_bw')?.setValue('300MB/s');
      component.rateLimitForm.get('max_client_write_bw')?.setValue('400MB/s');
      jest.spyOn(FormatterService.prototype, 'toBytes').mockReturnValue(100);

      const result = component._getRateLimitArgs();

      expect(result.enable_qos).toBe(true);
      expect(result.qos_type).toBe('PerClient');
      expect(result.max_client_read_bw).toBe(100);
      expect(result.max_client_write_bw).toBe(100);
    });

    it('should return the correct rate limit args for PerSharePerClient', () => {
      component.qosTypeVal = 'PerShare_PerClient';
      component.rateLimitForm.get('enable_qos')?.setValue(true);
      component.rateLimitForm.get('qos_type')?.setValue('PerShare_PerClient');
      component.rateLimitForm.get('max_export_read_bw')?.setValue('100MB/s');
      component.rateLimitForm.get('max_export_write_bw')?.setValue('200MB/s');
      component.rateLimitForm.get('max_client_read_bw')?.setValue('300MB/s');
      component.rateLimitForm.get('max_client_write_bw')?.setValue('400MB/s');
      jest.spyOn(FormatterService.prototype, 'toBytes').mockReturnValue(100);

      const result = component._getRateLimitArgs();

      expect(result.enable_qos).toBe(true);
      expect(result.qos_type).toBe('PerShare_PerClient');
      expect(result.max_export_read_bw).toBe(100);
      expect(result.max_export_write_bw).toBe(100);
      expect(result.max_client_read_bw).toBe(100);
      expect(result.max_client_write_bw).toBe(100);
    });
  });
});
