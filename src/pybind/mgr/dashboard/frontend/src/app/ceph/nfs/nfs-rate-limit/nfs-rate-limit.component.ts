import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { AbstractControl, UntypedFormControl, ValidationErrors, Validators } from '@angular/forms';
import _ from 'lodash';
import { NfsService } from '~/app/shared/api/nfs.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { bwTypeItem, NFSBwIopConfig, QOSType, QOSTypeItem } from '../models/nfs-cluster-config';
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
      this.loadConditionCheck();
    }
  }

  @Output()
  emitErrors = new EventEmitter();

  bwTypeArr: bwTypeItem[] = [];
  nfsClusterData: NFSBwIopConfig = {};
  qosTypeVal: string;
  rateLimitForm: CdFormGroup;
  qosType: QOSTypeItem[] = [];
  clusterId: string;
  allowQoS = false;
  clusterQosDisabled = false;
  showDisableWarning = false;
  showQostypeChangeNote = false;
  nfsExportdata: NFSBwIopConfig = {};

  constructor(private nfsService: NfsService, private formatterService: FormatterService) {
    this.bwTypeArr = this.nfsService.bwType;
  }

  ngOnInit(): void {
    this.createForm();
    this.qosType = this.nfsService.qosType;
    this.loadConditionCheck();
    this.rateLimitForm.controls['enable_qos'].valueChanges.subscribe((val) => {
      this.showMaxBwNote(val);
    });
    this.emitErrors.emit(this.rateLimitForm);
  }

  setValidation() {
    switch (this.qosTypeVal) {
      case 'PerShare': {
        this.rateLimitForm?.controls['max_export_read_bw'].addValidators(Validators.required);
        this.rateLimitForm?.controls['max_export_write_bw'].addValidators(Validators.required);
        this.rateLimitForm?.controls['max_client_write_bw'].removeValidators(Validators.required);
        this.rateLimitForm?.controls['max_client_read_bw'].removeValidators(Validators.required);
        break;
      }
      case 'PerClient': {
        this.rateLimitForm?.controls['max_client_write_bw'].addValidators(Validators.required);
        this.rateLimitForm?.controls['max_client_read_bw'].addValidators(Validators.required);
        this.rateLimitForm?.controls['max_export_read_bw'].removeValidators(Validators.required);
        this.rateLimitForm?.controls['max_export_write_bw'].removeValidators(Validators.required);
        break;
      }
      case 'PerShare_PerClient': {
        this.rateLimitForm?.controls['max_export_read_bw'].addValidators(Validators.required);
        this.rateLimitForm?.controls['max_export_write_bw'].addValidators(Validators.required);
        this.rateLimitForm?.controls['max_client_write_bw'].addValidators(Validators.required);
        this.rateLimitForm?.controls['max_client_read_bw'].addValidators(Validators.required);
      }
    }
  }

  showQOS() {
    this.allowQoS = this.type === 'export' && !this.nfsClusterData?.enable_qos;
  }

  loadConditionCheck() {
    this.showDisableWarning = false;

    const isCLusterType = this.type === 'cluster';
    const isExportType = this.type === 'export';
    const clusterQOSEnabled = this.nfsClusterData?.enable_qos || false;
    const exportQOSEnabled = this.nfsExportdata?.enable_qos || false;

    this.allowQoS = true;

    // Handle type 'cluster'
    isCLusterType &&
      clusterQOSEnabled &&
      ((this.clusterQosDisabled = false), this.isEdit && this.setFormData(this.nfsClusterData));

    // Handle type 'export'
    isExportType &&
      (clusterQOSEnabled && this.isEdit && this.setFormData(this.nfsExportdata),
      !clusterQOSEnabled &&
        exportQOSEnabled &&
        (this.registerQoSChange(this.nfsClusterData?.qos_type),
        this.setFormData(this.nfsExportdata),
        this.showDisabledField()),
      !clusterQOSEnabled && !exportQOSEnabled && (this.allowQoS = false));
  }

  createForm() {
    this.rateLimitForm = new CdFormGroup({
      enable_qos: new UntypedFormControl(false),
      qos_type: new UntypedFormControl(''),
      bwType: new UntypedFormControl('Individual'),
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
      ])
    });
  }

  showDisabledField() {
    this.clusterQosDisabled = true;
    this.qosTypeVal = 'PerShare_PerClient';
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

  setFormData(data: NFSBwIopConfig) {
    this.rateLimitForm?.get('enable_qos')?.setValue(data?.enable_qos);
    this.rateLimitForm?.get('qos_type')?.setValue(data?.qos_type);
    const fields = [
      'max_export_read_bw',
      'max_export_write_bw',
      'max_client_read_bw',
      'max_client_write_bw'
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
    if (this._isRateLimitFormDirty()) return this._getRateLimitArgs();
    return null;
  }

  /**
   * Check if the user rate limit has been modified.
   * @return {Boolean} Returns TRUE if the user rate limit has been modified.
   */
  _isRateLimitFormDirty(): boolean {
    return [
      'bwType',
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

  /**
   * Helper function to get the arguments for the API request when the user
   * rate limit configuration has been modified.
   */
  _getRateLimitArgs(): NFSBwIopConfig {
    const formValues = {
      enable_qos: this.rateLimitForm.getValue('enable_qos'),
      qos_type: this.rateLimitForm.getValue('qos_type'),
      max_export_write_bw: this.rateLimitForm.getValue('max_export_write_bw'),
      max_export_read_bw: this.rateLimitForm.getValue('max_export_read_bw'),
      max_client_write_bw: this.rateLimitForm.getValue('max_client_write_bw'),
      max_client_read_bw: this.rateLimitForm.getValue('max_client_read_bw')
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

    result['enable_qos'] = formValues.enable_qos;
    result['qos_type'] = formValues.qos_type;

    switch (this.qosTypeVal) {
      case 'PerShare': {
        result['max_export_read_bw'] = this.formatterService.toBytes(formValues.max_export_read_bw);
        result['max_export_write_bw'] = this.formatterService.toBytes(
          formValues.max_export_write_bw
        );
        break;
      }
      case 'PerClient': {
        result['max_client_write_bw'] = this.formatterService.toBytes(
          formValues.max_client_write_bw
        );
        result['max_client_read_bw'] = this.formatterService.toBytes(formValues.max_client_read_bw);
        break;
      }
      case 'PerShare_PerClient': {
        result['max_export_read_bw'] = this.formatterService.toBytes(formValues.max_export_read_bw);
        result['max_export_write_bw'] = this.formatterService.toBytes(
          formValues.max_export_write_bw
        );
        result['max_client_write_bw'] = this.formatterService.toBytes(
          formValues.max_client_write_bw
        );
        result['max_client_read_bw'] = this.formatterService.toBytes(formValues.max_client_read_bw);
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
  }

  getQoSTypeHelper(qosType: string) {
    const qosTypeItem = this.qosType.find(
      (currentQosTypeItem: QOSTypeItem) => qosType === currentQosTypeItem.value
    );
    return _.isObjectLike(qosTypeItem) ? qosTypeItem.help : '';
  }

  showMaxBwNote(val: boolean) {
    if (val) {
      // adding required as per the qos type to handle parent component submit errors.
      this.setValidation();
    } else {
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
    this.showDisableWarning = false;
    if (!val && this.isEdit == true && this.type == 'cluster' && !_.isEmpty(this.nfsClusterData)) {
      this.showDisableWarning = true;
    }
  }
}
