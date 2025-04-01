import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NfsRateLimitComponent } from './nfs-rate-limit.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { Validators } from '@angular/forms';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { NFS_TYPE } from '../models/nfs-cluster-config';

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
      component.type = NFS_TYPE.cluster;
      component.nfsClusterData = { enable_bw_control: true } as any;
      spyOn(component, 'setFormData');
      component.loadConditionCheck();
      expect(component.showDisableWarning).toBeFalsy();
      expect(component.allowQoS).toBeTruthy();
      expect(component.clusterQosDisabled).toBeFalsy();
      expect(component.setFormData).toHaveBeenCalledWith(component.nfsClusterData);
    });

    it('should handle type "export" with cluster QoS enabled', () => {
      component.type = NFS_TYPE.export;
      component.nfsClusterData = { enable_bw_control: true } as any;
      spyOn(component, 'setFormData');
      component.loadConditionCheck();
      expect(component.showDisableWarning).toBeFalsy();
      expect(component.allowQoS).toBeTruthy();
      expect(component.setFormData).toHaveBeenCalledWith(component.nfsExportdata);
    });

    it('should handle type "export" with export QoS enabled and cluster QoS disabled', () => {
      component.type = NFS_TYPE.export;
      component.nfsClusterData = { enable_bw_control: false } as any;
      component.nfsExportdata = { enable_bw_control: true } as any;
      spyOn(component, 'registerQoSChange');
      spyOn(component, 'setFormData');
      spyOn(component, 'showDisabledField');
      component.loadConditionCheck();
      expect(component.showDisableWarning).toBeFalsy();
      expect(component.allowQoS).toBeTruthy();
      expect(component.registerQoSChange).toHaveBeenCalledWith(component.nfsClusterData.qos_type);
      expect(component.setFormData).toHaveBeenCalledWith(component.nfsExportdata);
      expect(component.showDisabledField).toHaveBeenCalled();
    });

    it('should handle type "export" with both cluster and export QoS disabled', () => {
      component.type = NFS_TYPE.export;
      component.nfsClusterData = { enable_bw_control: false } as any;
      component.nfsExportdata = { enable_bw_control: false } as any;
      component.loadConditionCheck();
      expect(component.showDisableWarning).toBeFalsy();
      expect(component.allowQoS).toBeFalsy();
    });

    it('should handle type "cluster" with IOPS enabled', () => {
      component.type = NFS_TYPE.cluster;
      component.nfsClusterData = { enable_iops_control: true } as any;
      spyOn(component, 'setFormData');

      component.loadConditionCheck();

      expect(component.showDisableWarning).toBeFalsy();
      expect(component.allowIops).toBeTruthy();
      expect(component.clusterIopsDisabled).toBeFalsy();
      expect(component.setFormData).toHaveBeenCalledWith(component.nfsClusterData);
    });

    it('should handle type "export" with IOPS enabled for export and cluster disabled', () => {
      component.type = NFS_TYPE.export;
      component.nfsClusterData = { enable_iops_control: false } as any;
      component.nfsExportdata = { enable_iops_control: true } as any;
      spyOn(component, 'registerQoSIOPSChange');
      spyOn(component, 'setFormData');
      spyOn(component, 'showDisabledIopsField');

      component.loadConditionCheck();

      expect(component.showDisableWarning).toBeFalsy();
      expect(component.allowIops).toBeTruthy();
      expect(component.registerQoSIOPSChange).toHaveBeenCalledWith(
        component.nfsClusterData.qos_type
      );
      expect(component.setFormData).toHaveBeenCalledWith(component.nfsExportdata);
      expect(component.showDisabledIopsField).toHaveBeenCalled();
    });
  });

  describe('setValidation', () => {
    beforeEach(() => {
      component.createForm();
    });

    it('should set validators for PerShare QoS type and remove for others', () => {
      component.qosTypeVal = 'PerShare';
      component.rateLimitForm.controls['enable_qos'].setValue(true);
      jest.spyOn(component.rateLimitForm.controls['max_export_read_bw'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_export_write_bw'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_write_bw'], 'removeValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_read_bw'], 'removeValidators');
      component.setValidation();
      expect(
        component.rateLimitForm.controls['max_export_read_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_export_write_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_write_bw'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_read_bw'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
    });

    it('should set validators for PerClient QoS type and remove for others', () => {
      component.qosTypeVal = 'PerClient';
      component.rateLimitForm.controls['enable_qos'].setValue(true);
      jest.spyOn(component.rateLimitForm.controls['max_client_read_bw'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_write_bw'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_export_read_bw'], 'removeValidators');
      jest.spyOn(component.rateLimitForm.controls['max_export_write_bw'], 'removeValidators');
      component.setValidation();
      expect(
        component.rateLimitForm.controls['max_client_read_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_write_bw'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_export_read_bw'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_export_write_bw'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
    });

    it('should set validators for PerShare_PerClient QoS type and remove for others', () => {
      component.qosTypeVal = 'PerShare_PerClient';
      component.rateLimitForm.controls['enable_qos'].setValue(true);
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

    it('should set validators for PerShare QoS type for IOPS and remove for others', () => {
      component.qosiopsTypeVal = 'PerShare';
      component.rateLimitForm.controls['enable_ops'].setValue(true);
      jest.spyOn(component.rateLimitForm.controls['max_export_iops'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_iops'], 'removeValidators');
      component.setValidation();
      expect(
        component.rateLimitForm.controls['max_export_iops'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_iops'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
    });

    it('should set validators for PerClient QoS type for IOPS and remove for others', () => {
      component.qosiopsTypeVal = 'PerClient';
      component.rateLimitForm.controls['enable_ops'].setValue(true);
      jest.spyOn(component.rateLimitForm.controls['max_client_iops'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_export_iops'], 'removeValidators');
      component.setValidation();
      expect(
        component.rateLimitForm.controls['max_client_iops'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_export_iops'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
    });

    it('should set validators for PerShare_PerClient QoS type for IOPS and remove for others', () => {
      component.qosiopsTypeVal = 'PerShare_PerClient';
      component.rateLimitForm.controls['enable_ops'].setValue(true);
      jest.spyOn(component.rateLimitForm.controls['max_export_iops'], 'addValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_iops'], 'addValidators');
      component.setValidation();
      expect(
        component.rateLimitForm.controls['max_export_iops'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_iops'].addValidators
      ).toHaveBeenCalledWith(Validators.required);
    });
  });

  describe('showMaxBwNote', () => {
    beforeEach(() => {
      component.createForm();
    });

    it('should add validators when QoS is enabled (val is true)', () => {
      jest.spyOn(component, 'setValidation');
      component.showMaxBwNote(true, 'bw');
      expect(component.setValidation).toHaveBeenCalled();
    });

    it('should remove validators for bandwidth fields when QoS is disabled and type is "bw"', () => {
      jest.spyOn(component.rateLimitForm.controls['max_export_read_bw'], 'removeValidators');
      jest.spyOn(component.rateLimitForm.controls['max_export_write_bw'], 'removeValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_read_bw'], 'removeValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_write_bw'], 'removeValidators');
      component.showMaxBwNote(false, 'bw');
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

    it('should remove validators for iops fields when QoS is disabled and type is "iops"', () => {
      jest.spyOn(component.rateLimitForm.controls['max_export_iops'], 'removeValidators');
      jest.spyOn(component.rateLimitForm.controls['max_client_iops'], 'removeValidators');
      component.showMaxBwNote(false, 'iops');
      expect(
        component.rateLimitForm.controls['max_export_iops'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
      expect(
        component.rateLimitForm.controls['max_client_iops'].removeValidators
      ).toHaveBeenCalledWith(Validators.required);
    });

    it('should not call removeValidators if the type does not match "bw" or "iops"', () => {
      const removeValidatorsSpy = jest.spyOn(
        component.rateLimitForm.controls['max_export_read_bw'],
        'removeValidators'
      );
      component.showMaxBwNote(false, 'unknownType');
      expect(removeValidatorsSpy).not.toHaveBeenCalled();
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

  describe('setFormData', () => {
    beforeEach(() => {
      component.createForm();
    });

    it('should set form data correctly', () => {
      const data = {
        enable_bw_control: true,
        enable_iops_control: false,
        qos_type: 'PerShare',
        max_export_read_bw: 300,
        max_export_write_bw: 400
      };
      component.setFormData(data);
      expect(component.rateLimitForm.get('enable_qos')?.value).toBe(data.enable_bw_control);
      expect(component.rateLimitForm.get('enable_ops')?.value).toBe(data.enable_iops_control);
      expect(component.rateLimitForm.get('qos_type')?.value).toBe(data.qos_type);
      expect(component.rateLimitForm.get('max_export_read_bw')?.value).toBe(300);
      expect(component.rateLimitForm.get('max_export_write_bw')?.value).toBe(400);
    });
  });

  describe('getRateLimitFormValue', () => {
    it('should return rate limit args if form is dirty', () => {
      jest.spyOn(component, '_isRateLimitFormDirty').mockReturnValue(true);
      const rateLimitArgs = { enable_qos: true } as any;
      jest.spyOn(component, '_getRateLimitArgs').mockReturnValue(rateLimitArgs);
      const result = component.getRateLimitFormValue();
      expect(result).toBe(rateLimitArgs);
      expect(component._getRateLimitArgs).toHaveBeenCalled();
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
