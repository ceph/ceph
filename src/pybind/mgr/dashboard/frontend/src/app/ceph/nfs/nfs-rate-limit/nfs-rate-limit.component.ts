import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Input,
  OnInit,
  Output
} from '@angular/core';
import {
  AbstractControl,
  ReactiveFormsModule,
  UntypedFormControl,
  ValidationErrors,
  Validators
} from '@angular/forms';
import { InputModule, NumberModule, SelectModule, ToggleModule } from 'carbon-components-angular';
import { NfsService } from '~/app/shared/api/nfs.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FormatterService } from '~/app/shared/services/formatter.service';
import {
  BW_UNITS,
  DEFAULT_BW_UNIT,
  MAX_BW_BYTES,
  MIN_BW_BYTES,
  NFS_TYPE,
  NFSBwIopConfig,
  QOSType,
  QOSTypeItem,
  UNIT_TO_BYTES
} from '../models/nfs-cluster-config';
import { SharedModule } from '~/app/shared/shared.module';

@Component({
  selector: 'cd-nfs-rate-limit-form',
  templateUrl: './nfs-rate-limit.component.html',
  styleUrls: ['./nfs-rate-limit.component.scss'],
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    ReactiveFormsModule,
    SharedModule,
    ToggleModule,
    SelectModule,
    InputModule,
    NumberModule
  ]
})
export class NfsRateLimitComponent implements OnInit {
  @Input() action: string;
  @Input() isEdit: boolean;
  @Input() type: string;

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
  nfsClusterData: NFSBwIopConfig = {};
  qosTypeVal: string;
  rateLimitForm: CdFormGroup;
  qosType: QOSTypeItem[] = [];
  qosTypeValue = QOSType;
  nfsType = NFS_TYPE;
  allowQoS = false;
  allowIops = false;
  clusterQosDisabled = false;
  clusterIopsDisabled = false;
  showDisableWarning = false;
  showQostypeChangeNote = false;
  nfsExportdata: NFSBwIopConfig = {};
  bwEnabled = false;
  iopsEnabled = false;
  bwUnits = BW_UNITS;

  constructor(
    private nfsService: NfsService,
    private formatterService: FormatterService,
    private changeDetectorRef: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.createForm();
    this.qosType = this.nfsService.qosType;
    this.loadConditionCheck();
    this.emitErrors.emit(this.rateLimitForm);
  }

  onBandwidthToggle(): void {
    this.bwEnabled = !this.bwEnabled;
    this.rateLimitForm.get('enable_qos').setValue(this.bwEnabled);
    this.rateLimitForm.get('enable_qos').markAsDirty();
    this.showMaxBwNote(this.bwEnabled, 'bw');
  }

  onIopsToggle(): void {
    this.iopsEnabled = !this.iopsEnabled;
    this.rateLimitForm.get('enable_ops').setValue(this.iopsEnabled);
    this.rateLimitForm.get('enable_ops').markAsDirty();
    this.showMaxBwNote(this.iopsEnabled, 'iops');
  }

  setValidation() {
    const bwFields = [
      'max_export_read_bw',
      'max_export_write_bw',
      'max_client_write_bw',
      'max_client_read_bw'
    ];
    const iopsFields = ['max_export_iops', 'max_client_iops'];

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
          break;
        }
      }
      bwFields.forEach((f) => this.rateLimitForm?.get(f)?.updateValueAndValidity());
    }
    if (this.rateLimitForm?.get('enable_ops').value) {
      switch (this.qosTypeVal) {
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
          break;
        }
      }
      iopsFields.forEach((f) => this.rateLimitForm?.get(f)?.updateValueAndValidity());
    }

    this.changeDetectorRef.detectChanges();
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
        (this.registerQoSChange(this.nfsClusterData?.qos_type),
        this.setFormData(this.nfsExportdata),
        this.showDisabledIopsField()),
      !clusterIopsEnabled && !exportIopsEnabled && (this.allowIops = false));
  }

  createForm() {
    this.rateLimitForm = new CdFormGroup({
      qos_type: new UntypedFormControl(QOSType.PerShare),
      enable_qos: new UntypedFormControl(false),
      max_export_write_bw: new UntypedFormControl(null, [
        Validators.min(1),
        this.bwRangeValidator('max_export_write_bw_unit')
      ]),
      max_export_write_bw_unit: new UntypedFormControl(DEFAULT_BW_UNIT),
      max_export_read_bw: new UntypedFormControl(null, [
        Validators.min(1),
        this.bwRangeValidator('max_export_read_bw_unit')
      ]),
      max_export_read_bw_unit: new UntypedFormControl(DEFAULT_BW_UNIT),
      max_client_write_bw: new UntypedFormControl(null, [
        Validators.min(1),
        this.bwRangeValidator('max_client_write_bw_unit')
      ]),
      max_client_write_bw_unit: new UntypedFormControl(DEFAULT_BW_UNIT),
      max_client_read_bw: new UntypedFormControl(null, [
        Validators.min(1),
        this.bwRangeValidator('max_client_read_bw_unit')
      ]),
      max_client_read_bw_unit: new UntypedFormControl(DEFAULT_BW_UNIT),
      enable_ops: new UntypedFormControl(false),
      max_export_iops: new UntypedFormControl(null, [Validators.min(10), Validators.max(1638400)]),
      max_client_iops: new UntypedFormControl(null, [Validators.min(10), Validators.max(1638400)])
    });
  }

  showValidationErrors(): boolean {
    if (!this.rateLimitForm?.invalid) {
      return true;
    }
    this.rateLimitForm.markAllAsTouched();
    Object.keys(this.rateLimitForm.controls).forEach((name) => {
      const control = this.rateLimitForm.get(name);
      control?.markAsDirty();
      control?.updateValueAndValidity();
    });
    this.changeDetectorRef.markForCheck();
    return false;
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
    this.qosTypeVal = QOSType.PerSharePerClient;
    const fields = ['enable_ops', 'max_export_iops', 'max_client_iops'];
    fields.forEach((field) => {
      this.rateLimitForm?.get(field)?.disable();
    });
  }

  setFormData(data: NFSBwIopConfig) {
    this.bwEnabled = !!data?.enable_bw_control;
    this.iopsEnabled = !!data?.enable_iops_control;
    this.rateLimitForm?.get('enable_qos')?.setValue(this.bwEnabled);
    this.rateLimitForm?.get('enable_ops')?.setValue(this.iopsEnabled);
    this.rateLimitForm?.get('qos_type')?.setValue(data?.qos_type || QOSType.PerShare);

    const bwFields = [
      'max_export_read_bw',
      'max_export_write_bw',
      'max_client_read_bw',
      'max_client_write_bw'
    ];
    bwFields.forEach((field) => {
      const bytes = this.formatterService.toBytes(data?.[field]);
      const { value, unit } = this.bytesToValueUnit(bytes);
      this.rateLimitForm?.get(field)?.setValue(value);
      this.rateLimitForm?.get(field + '_unit')?.setValue(unit);
    });

    const iopsFields = ['max_export_iops', 'max_client_iops'];
    iopsFields.forEach((field) => {
      this.rateLimitForm?.get(field)?.setValue(data?.[field] || null);
    });
  }

  private bytesToValueUnit(bytes: number): { value: number; unit: string } {
    if (!bytes || bytes <= 0) {
      return { value: null, unit: DEFAULT_BW_UNIT };
    }
    const gib = 1024 ** 3;
    const mib = 1024 ** 2;
    const kib = 1024;
    if (bytes >= gib && bytes % gib === 0) {
      return { value: bytes / gib, unit: 'GiB/s' };
    }
    if (bytes >= mib && bytes % mib === 0) {
      return { value: bytes / mib, unit: 'MiB/s' };
    }
    return { value: Math.round(bytes / kib), unit: 'KiB/s' };
  }

  private valueUnitToBytes(value: number, unit: string): number {
    if (!value || value <= 0) {
      return 0;
    }
    return value * (UNIT_TO_BYTES[unit] || 1);
  }

  bwRangeValidator(unitControlName: string) {
    return (control: AbstractControl): ValidationErrors | null => {
      if (!control.value || !this.rateLimitForm) {
        return null;
      }
      const unit = this.rateLimitForm.get(unitControlName)?.value || DEFAULT_BW_UNIT;
      const bytes = this.valueUnitToBytes(control.value, unit);
      if (bytes < MIN_BW_BYTES) {
        return { bwMinSize: true };
      }
      if (bytes > MAX_BW_BYTES) {
        return { bwMaxSize: true };
      }
      return null;
    };
  }

  getRateLimitFormValue() {
    return this._getRateLimitArgs();
  }

  getRateLimitOpsFormValue() {
    return this._getRateLimitOpsArgs();
  }

  _getRateLimitArgs(): NFSBwIopConfig {
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

    const enableQos = this.rateLimitForm.get('enable_qos').value;
    if (!enableQos) {
      return result;
    }

    result['enable_qos'] = true;
    result['qos_type'] = this.rateLimitForm.get('qos_type').value;

    switch (this.qosTypeVal) {
      case QOSType.PerShare: {
        result['max_export_read_bw'] = this.valueUnitToBytes(
          this.rateLimitForm.get('max_export_read_bw').value,
          this.rateLimitForm.get('max_export_read_bw_unit').value
        );
        result['max_export_write_bw'] = this.valueUnitToBytes(
          this.rateLimitForm.get('max_export_write_bw').value,
          this.rateLimitForm.get('max_export_write_bw_unit').value
        );
        break;
      }
      case QOSType.PerClient: {
        result['max_client_write_bw'] = this.valueUnitToBytes(
          this.rateLimitForm.get('max_client_write_bw').value,
          this.rateLimitForm.get('max_client_write_bw_unit').value
        );
        result['max_client_read_bw'] = this.valueUnitToBytes(
          this.rateLimitForm.get('max_client_read_bw').value,
          this.rateLimitForm.get('max_client_read_bw_unit').value
        );
        break;
      }
      case QOSType.PerSharePerClient: {
        result['max_export_read_bw'] = this.valueUnitToBytes(
          this.rateLimitForm.get('max_export_read_bw').value,
          this.rateLimitForm.get('max_export_read_bw_unit').value
        );
        result['max_export_write_bw'] = this.valueUnitToBytes(
          this.rateLimitForm.get('max_export_write_bw').value,
          this.rateLimitForm.get('max_export_write_bw_unit').value
        );
        result['max_client_write_bw'] = this.valueUnitToBytes(
          this.rateLimitForm.get('max_client_write_bw').value,
          this.rateLimitForm.get('max_client_write_bw_unit').value
        );
        result['max_client_read_bw'] = this.valueUnitToBytes(
          this.rateLimitForm.get('max_client_read_bw').value,
          this.rateLimitForm.get('max_client_read_bw_unit').value
        );
        break;
      }
    }
    return result;
  }

  _getRateLimitOpsArgs(): NFSBwIopConfig {
    const result: NFSBwIopConfig = {
      enable_ops: false,
      qos_type: '',
      max_export_iops: 0,
      max_client_iops: 0
    };

    const enableOps = this.rateLimitForm.get('enable_ops').value;
    if (!enableOps) {
      return result;
    }

    result['enable_ops'] = true;
    result['qos_type'] = this.rateLimitForm.get('qos_type').value;

    switch (this.qosTypeVal) {
      case QOSType.PerShare: {
        result['max_export_iops'] = this.rateLimitForm.get('max_export_iops').value;
        break;
      }
      case QOSType.PerClient: {
        result['max_client_iops'] = this.rateLimitForm.get('max_client_iops').value;
        break;
      }
      case QOSType.PerSharePerClient: {
        result['max_export_iops'] = this.rateLimitForm.get('max_export_iops').value;
        result['max_client_iops'] = this.rateLimitForm.get('max_client_iops').value;
        break;
      }
    }
    return result;
  }

  registerQoSChange(val: string) {
    this.showQostypeChangeNote = false;
    this.qosTypeVal = Object.values(QOSType).find((qos) => qos == val) || QOSType.PerShare;
    this.setValidation();
    if (this.isEdit && this.nfsClusterData?.qos_type !== val) {
      this.showQostypeChangeNote = true;
    }
    this.rateLimitForm?.get('qos_type').updateValueAndValidity({ onlySelf: true });
    this.changeDetectorRef.detectChanges();
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
    this.changeDetectorRef.markForCheck();
    this.changeDetectorRef.detectChanges();
  }
}
