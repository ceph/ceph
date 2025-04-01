import { ChangeDetectorRef, Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { AbstractControl, UntypedFormControl, ValidationErrors, Validators } from '@angular/forms';
import _ from 'lodash';
import { NfsService } from '~/app/shared/api/nfs.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FormatterService } from '~/app/shared/services/formatter.service';
import {
  bwTypeItem,
  NFS_TYPE,
  NFSBwIopConfig,
  QOSType,
  QOSTypeItem
} from '../models/nfs-cluster-config';
import { CdValidators } from '~/app/shared/forms/cd-validators';

@Component({
  selector: 'cd-nfs-rate-limit-form',
  templateUrl: './nfs-rate-limit.component.html',
  styleUrls: ['./nfs-rate-limit.component.scss']
})
export class NfsRateLimitComponent implements OnInit {
  @Input() action: string;
  @Input() isEdit: boolean;
  @Input() type: String;
  qosiopsTypeVal: string;

  @Input() set exportDataConfig(value: NFSBwIopConfig) {
    if (value) {
      this.nfsExportdata = value;
      this.loadConditionCheck();
    }
  }
  @Input() set clusterDataConfig(value: NFSBwIopConfig) {
    if (value) {
      this.nfsClusterData = value;
      this.registerQoSChange(this.nfsClusterData.qos_type);
      this.registerQoSIOPSChange(this.nfsClusterData.qos_type);
      this.loadConditionCheck();
    }
  }

  @Output()
  emitErrors = new EventEmitter();
  ngDataReady = new EventEmitter<any>();
  bwTypeArr: bwTypeItem[] = [];
  nfsClusterData: NFSBwIopConfig = {};
  qosTypeVal: string;
  rateLimitForm: CdFormGroup;
  qosType: QOSTypeItem[] = [];
  qosIopsType: QOSTypeItem[] = [];
  qosTypeValue = QOSType;
  nfsType = NFS_TYPE;
  clusterId: string;
  allowQoS = false;
  allowIops = false;
  clusterQosDisabled = false;
  clusterIopsDisabled = false;
  showDisableWarning = false;
  showQostypeChangeNote = false;
  nfsExportdata: NFSBwIopConfig = {};

  constructor(
    private nfsService: NfsService,
    private formatterService: FormatterService,
    private chnageDetectorRef: ChangeDetectorRef
  ) {
    this.bwTypeArr = this.nfsService.bwType;
  }

  ngOnInit(): void {
    this.createForm();
    this.qosType = this.nfsService.qosType;
    this.qosIopsType = this.nfsService.qosiopsType;
    this.loadConditionCheck();
    this.rateLimitForm.controls['enable_qos'].valueChanges.subscribe((val) => {
      this.showMaxBwNote(val, 'bw');
    });
    this.rateLimitForm.controls['enable_ops'].valueChanges.subscribe((val) => {
      this.showMaxBwNote(val, 'iops');
    });
    this.emitErrors.emit(this.rateLimitForm);
  }
  setValidation() {
    if (this.rateLimitForm?.get('enable_qos').value) {
      switch (this.qosTypeVal) {
        case QOSType.PerShare: {
          this.rateLimitForm?.controls['max_export_read_bw'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_export_write_bw'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_client_write_bw'].removeValidators(Validators.required);
          this.rateLimitForm?.controls['max_client_read_bw'].removeValidators(Validators.required);
          break;
        }
        case QOSType.PerClient: {
          this.rateLimitForm?.controls['max_client_write_bw'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_client_read_bw'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_export_read_bw'].removeValidators(Validators.required);
          this.rateLimitForm?.controls['max_export_write_bw'].removeValidators(Validators.required);
          break;
        }
        case QOSType.PerSharePerClient: {
          this.rateLimitForm?.controls['max_export_read_bw'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_export_write_bw'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_client_write_bw'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_client_read_bw'].addValidators(Validators.required);
        }
      }
    }
    if (this.rateLimitForm?.get('enable_ops').value) {
      switch (this.qosiopsTypeVal) {
        case QOSType.PerShare: {
          this.rateLimitForm?.controls['max_export_iops'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_client_iops'].removeValidators(Validators.required);
          break;
        }
        case QOSType.PerClient: {
          this.rateLimitForm?.controls['max_client_iops'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_export_iops'].removeValidators(Validators.required);
          break;
        }
        case QOSType.PerSharePerClient: {
          this.rateLimitForm?.controls['max_export_iops'].addValidators(Validators.required);
          this.rateLimitForm?.controls['max_client_iops'].addValidators(Validators.required);
        }
      }
    }

    this.chnageDetectorRef.detectChanges();
  }

  loadConditionCheck() {
    this.showDisableWarning = false;
    const isClusterType = this.type === NFS_TYPE.cluster;
    const isExportType = this.type === NFS_TYPE.export;
    const clusterQOSEnabled = this.nfsClusterData?.enable_bw_control || false;
    const exportQOSEnabled = this.nfsExportdata?.enable_bw_control || false;

    const clusterIopsEnabled = this.nfsClusterData?.enable_iops_control || false;
    const exportIopsEnabled = this.nfsExportdata?.enable_iops_control || false;

    this.allowQoS = true;
    this.allowIops = true;
    // Handle type 'cluster'
    isClusterType &&
      clusterQOSEnabled &&
      ((this.clusterQosDisabled = false), this.isEdit && this.setFormData(this.nfsClusterData));

    isClusterType &&
      clusterIopsEnabled &&
      ((this.clusterIopsDisabled = false), this.isEdit && this.setFormData(this.nfsClusterData));

    // Handle type 'export'
    isExportType &&
      (clusterQOSEnabled && this.isEdit && this.setFormData(this.nfsExportdata),
      !clusterQOSEnabled &&
        exportQOSEnabled &&
        (this.registerQoSChange(this.nfsClusterData?.qos_type),
        this.setFormData(this.nfsExportdata),
        this.showDisabledField()),
      !clusterQOSEnabled && !exportQOSEnabled && (this.allowQoS = false));

    isExportType &&
      (clusterIopsEnabled && this.isEdit && this.setFormData(this.nfsExportdata),
      !clusterIopsEnabled &&
        exportIopsEnabled &&
        (this.registerQoSIOPSChange(this.nfsClusterData?.qos_type),
        this.setFormData(this.nfsExportdata),
        this.showDisabledIopsField()),
      !clusterIopsEnabled && !exportIopsEnabled && (this.allowIops = false));
  }

  createForm() {
    this.rateLimitForm = new CdFormGroup({
      enable_qos: new UntypedFormControl(false),
      qos_type: new UntypedFormControl('', [this.qosTypeValidator.bind(this)]),
      max_export_read_bw: new UntypedFormControl(null, [
        CdValidators.composeIf({}, [this.rateLimitBytesMaxSizeValidator])
      ]),
      max_export_write_bw: new UntypedFormControl(null, [
        CdValidators.composeIf({}, [this.rateLimitBytesMaxSizeValidator])
      ]),
      max_client_read_bw: new UntypedFormControl(null, [
        CdValidators.composeIf({}, [this.rateLimitBytesMaxSizeValidator])
      ]),
      max_client_write_bw: new UntypedFormControl(null, [
        CdValidators.composeIf({}, [this.rateLimitBytesMaxSizeValidator])
      ]),
      enable_ops: new UntypedFormControl(false),
      qos_type_ops: new UntypedFormControl('', [this.qosTypeValidator.bind(this)]),
      max_export_iops: new UntypedFormControl(null, [Validators.min(10), Validators.max(16384)]),
      max_client_iops: new UntypedFormControl(null, [Validators.min(10), Validators.max(16384)])
    });
  }

  showDisabledField() {
    this.clusterQosDisabled = true;
    this.qosTypeVal = QOSType.PerSharePerClient;
    const fields = [
      'enable_qos',
      'max_export_read_bw',
      'max_export_write_bw',
      'max_client_read_bw',
      'max_client_write_bw'
    ];
    fields.forEach((field) => {
      this.rateLimitForm?.get(field)?.disable();
    });
  }

  showDisabledIopsField() {
    this.clusterIopsDisabled = true;
    this.qosiopsTypeVal = QOSType.PerSharePerClient;
    const fields = ['enable_ops', 'max_export_iops', 'max_client_iops'];
    fields.forEach((field) => {
      this.rateLimitForm?.get(field)?.disable();
    });
  }

  setFormData(data: NFSBwIopConfig) {
    this.rateLimitForm?.get('enable_qos')?.setValue(data?.enable_bw_control);
    this.rateLimitForm?.get('enable_ops')?.setValue(data?.enable_iops_control);
    this.rateLimitForm?.get('qos_type')?.setValue(data?.qos_type);
    this.rateLimitForm?.get('qos_type_ops')?.setValue(data?.qos_type);
    const fields = [
      'max_export_read_bw',
      'max_export_write_bw',
      'max_client_read_bw',
      'max_client_write_bw',
      'max_export_iops',
      'max_client_iops'
    ];
    fields.forEach((field) => {
      this.rateLimitForm?.get(field)?.setValue(this.formatterService.toBytes(data?.[field]));
    });
  }

  rateLimitBytesMaxSizeValidator(control: AbstractControl): ValidationErrors | null {
    return new FormatterService().performValidation(
      control,
      '^(\\d+(\\.\\d+)?)\\s*(B/s|K(B|iB/s)?|M(B|iB/s)?|G(B|iB/s)?|T(B|iB/s)?|P(B|iB/s)?)?$',
      { rateByteMaxSize: true },
      'nfsRateLimit'
    );
  }

  getbwTypeHelp(type: string) {
    const bwTypeItem = this.bwTypeArr.find((item: bwTypeItem) => type === item.value);
    return _.isObjectLike(bwTypeItem) ? bwTypeItem.help : '';
  }

  getRateLimitFormValue() {
    return this._getRateLimitArgs();
  }

  getRateLimitOpsFormValue() {
    return this._getRateLimitOpsArgs();
  }

  /**
   * Check if the user rate limit has been modified.
   * @return {Boolean} Returns TRUE if the user rate limit has been modified.
   */
  _isRateLimitFormDirty(): boolean {
    return [
      'enable_qos',
      'qos_type',
      'max_export_read_bw',
      'max_export_write_bw',
      'max_client_write_bw',
      'max_client_read_bw'
    ].some((path) => {
      return this.rateLimitForm.get(path).dirty;
    });
  }

  _isRateLimitOPSFormDirty(): boolean {
    return ['enable_ops', 'qos_type_ops', 'max_export_iops', 'max_client_iops'].some((path) => {
      return this.rateLimitForm.get(path).dirty;
    });
  }

  /**
   * Helper function to get the arguments for the API request when the user
   * rate limit configuration has been modified.
   */
  _getRateLimitArgs(): NFSBwIopConfig {
    const formValues = {
      enable_qos: this.rateLimitForm.get('enable_qos').value,
      qos_type: this.rateLimitForm.get('qos_type').value,
      max_export_write_bw: this.rateLimitForm.get('max_export_write_bw').value,
      max_export_read_bw: this.rateLimitForm.get('max_export_read_bw').value,
      max_client_write_bw: this.rateLimitForm.get('max_client_write_bw').value,
      max_client_read_bw: this.rateLimitForm.get('max_client_read_bw').value
    };

    const result: NFSBwIopConfig = {
      enable_qos: false,
      qos_type: '',
      max_export_write_bw: 0,
      max_export_read_bw: 0,
      max_client_write_bw: 0,
      max_client_read_bw: 0,
      max_export_combined_bw: 0,
      max_client_combined_bw: 0
    };

    // If QoS is enabled, update the corresponding QoS values
    if (formValues.enable_qos) {
      result['enable_qos'] = formValues.enable_qos;
      result['qos_type'] = formValues.qos_type;

      switch (this.qosTypeVal) {
        case QOSType.PerShare: {
          result['max_export_read_bw'] = this.formatterService.toBytes(
            formValues.max_export_read_bw
          );
          result['max_export_write_bw'] = this.formatterService.toBytes(
            formValues.max_export_write_bw
          );
          break;
        }
        case QOSType.PerClient: {
          result['max_client_write_bw'] = this.formatterService.toBytes(
            formValues.max_client_write_bw
          );
          result['max_client_read_bw'] = this.formatterService.toBytes(
            formValues.max_client_read_bw
          );
          break;
        }
        case QOSType.PerSharePerClient: {
          result['max_export_read_bw'] = this.formatterService.toBytes(
            formValues.max_export_read_bw
          );
          result['max_export_write_bw'] = this.formatterService.toBytes(
            formValues.max_export_write_bw
          );
          result['max_client_write_bw'] = this.formatterService.toBytes(
            formValues.max_client_write_bw
          );
          result['max_client_read_bw'] = this.formatterService.toBytes(
            formValues.max_client_read_bw
          );
          break;
        }
      }
    }
    return result;
  }

  _getRateLimitOpsArgs(): NFSBwIopConfig {
    const formValues = {
      enable_ops: this.rateLimitForm.get('enable_ops').value,
      qos_type: this.rateLimitForm.get('qos_type_ops').value,
      max_export_iops: this.rateLimitForm.get('max_export_iops').value,
      max_client_iops: this.rateLimitForm.get('max_client_iops').value
    };

    const result: NFSBwIopConfig = {
      enable_ops: false,
      qos_type: '',
      max_export_iops: 0,
      max_client_iops: 0
    };
    // If Ops is enabled, update the corresponding Ops values
    if (formValues.enable_ops) {
      result['enable_ops'] = formValues.enable_ops;
      result['qos_type'] = formValues.qos_type;

      switch (this.qosiopsTypeVal) {
        case QOSType.PerShare: {
          result['max_export_iops'] = this.formatterService.toBytes(formValues.max_export_iops);
          break;
        }
        case QOSType.PerClient: {
          result['max_client_iops'] = this.formatterService.toBytes(formValues.max_client_iops);
          break;
        }
        case QOSType.PerSharePerClient: {
          result['max_export_iops'] = this.formatterService.toBytes(formValues.max_export_iops);
          result['max_client_iops'] = this.formatterService.toBytes(formValues.max_client_iops);
          break;
        }
      }
    }
    return result;
  }

  registerQoSChange(val: string) {
    this.showQostypeChangeNote = false;
    this.qosTypeVal = Object.values(QOSType).find((qos) => qos == val);
    this.setValidation();
    if (this.isEdit && this.nfsClusterData?.qos_type !== val) {
      this.showQostypeChangeNote = true;
    }
    this.rateLimitForm?.get('qos_type').updateValueAndValidity();
    this.rateLimitForm?.get('qos_type_ops').updateValueAndValidity();
    this.chnageDetectorRef.detectChanges();
  }

  qosTypeValidator(): ValidationErrors | null {
    const qosType = this.rateLimitForm?.get('qos_type')?.value;
    const qosTypeOps = this.rateLimitForm?.get('qos_type_ops')?.value;
    const enable_qos = this.rateLimitForm?.get('enable_qos')?.value;
    const enable_ops = this.rateLimitForm?.get('enable_ops')?.value;

    if (enable_qos === true && enable_ops === true) {
      if (qosType !== qosTypeOps) {
        return { qosTypeInvalid: true };
      }
    }
    return null;
  }

  registerQoSIOPSChange(val: string) {
    this.qosiopsTypeVal = Object.values(QOSType).find((qos) => qos == val);
    this.setValidation();
    if (this.isEdit && this.nfsClusterData?.qos_type !== val) {
      this.showQostypeChangeNote = true;
    }
    this.rateLimitForm?.get('qos_type_ops').updateValueAndValidity();
    this.rateLimitForm?.get('qos_type').updateValueAndValidity();
    this.chnageDetectorRef.detectChanges();
  }

  getQoSTypeHelper(qosType: string) {
    const qosTypeItem = this.qosType.find(
      (currentQosTypeItem: QOSTypeItem) => qosType === currentQosTypeItem.value
    );
    return _.isObjectLike(qosTypeItem) ? qosTypeItem.help : '';
  }

  getQoSTypeIOPsHelper(qosType: string) {
    const qosTypeItem = this.qosIopsType.find(
      (currentQosTypeItem: QOSTypeItem) => qosType === currentQosTypeItem.value
    );
    return _.isObjectLike(qosTypeItem) ? qosTypeItem.help : '';
  }

  showMaxBwNote(val: boolean, type: string) {
    if (val) {
      // adding required as per the qos type to handle parent component submit errors.
      this.setValidation();
    } else {
      if (type === 'bw') {
        const fields = [
          'max_export_read_bw',
          'max_export_write_bw',
          'max_client_read_bw',
          'max_client_write_bw'
        ];
        fields.forEach((field) => {
          this.rateLimitForm?.get(field)?.removeValidators(Validators.required);
        });
      }
      if (type === 'iops') {
        const fields = ['max_export_iops', 'max_client_iops'];
        fields.forEach((field) => {
          this.rateLimitForm?.get(field)?.removeValidators(Validators.required);
        });
      }
    }
  }
}
