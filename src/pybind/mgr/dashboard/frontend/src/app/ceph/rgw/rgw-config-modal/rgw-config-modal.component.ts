import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { AbstractControl, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import {
  rgwBucketEncryptionModel,
  KMS_PROVIDER,
  ENCRYPTION_TYPE
} from '../models/rgw-bucket-encryption';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { KmipConfig, VaultConfig } from '~/app/shared/models/rgw-encryption-config-keys';

@Component({
  selector: 'cd-rgw-config-modal',
  templateUrl: './rgw-config-modal.component.html',
  styleUrls: ['./rgw-config-modal.component.scss']
})
export class RgwConfigModalComponent implements OnInit {
  readonly vaultAddress = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{4}$/;

  kmsProviders: string[];

  configForm: CdFormGroup;

  @Output()
  submitAction = new EventEmitter();
  authMethods: string[];
  secretEngines: string[];

  selectedEncryptionConfigValues: any = {};
  allEncryptionConfigValues: any = [];
  editing = false;
  action: string;
  table: TableComponent;
  ENCRYPTION_TYPE = ENCRYPTION_TYPE;
  KMS_PROVIDER = KMS_PROVIDER;
  constructor(
    private formBuilder: CdFormBuilder,
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private rgwBucketService: RgwBucketService,
    private notificationService: NotificationService
  ) {
    this.createForm();
  }
  ngOnInit(): void {
    this.kmsProviders = rgwBucketEncryptionModel.kmsProviders;
    this.authMethods = rgwBucketEncryptionModel.authMethods;
    this.secretEngines = rgwBucketEncryptionModel.secretEngines;
    if (this.editing && this.selectedEncryptionConfigValues) {
      const patchValues = {
        addr: this.selectedEncryptionConfigValues['addr'],
        encryptionType: this.selectedEncryptionConfigValues['encryption_type'],
        kms_provider: this.selectedEncryptionConfigValues['backend'],
        auth: this.selectedEncryptionConfigValues['auth'],
        secret_engine: this.selectedEncryptionConfigValues['secret_engine'],
        secret_path: this.selectedEncryptionConfigValues['prefix'],
        namespace: this.selectedEncryptionConfigValues['namespace'],
        kms_key_template: this.selectedEncryptionConfigValues['kms_key_template'],
        s3_key_template: this.selectedEncryptionConfigValues['s3_key_template'],
        username: this.selectedEncryptionConfigValues['username'],
        password: this.selectedEncryptionConfigValues['password'],
        ssl_cert:
          this.selectedEncryptionConfigValues['backend'] === KMS_PROVIDER.VAULT
            ? this.selectedEncryptionConfigValues['ssl_cacert']
            : this.selectedEncryptionConfigValues['ca_path'],
        client_cert:
          this.selectedEncryptionConfigValues['backend'] === KMS_PROVIDER.VAULT
            ? this.selectedEncryptionConfigValues['ssl_clientcert']
            : this.selectedEncryptionConfigValues['client_cert'],
        client_key:
          this.selectedEncryptionConfigValues['backend'] === KMS_PROVIDER.VAULT
            ? this.selectedEncryptionConfigValues['ssl_clientkey']
            : this.selectedEncryptionConfigValues['client_key']
      };
      this.configForm.patchValue(patchValues);
      this.configForm.get('kms_provider').disable();
    }
    this.checkKmsProviders();
  }

  setKmsProvider() {
    const selectedEncryptionType = this.configForm.get('encryptionType').value;
    this.kmsProviders =
      selectedEncryptionType === ENCRYPTION_TYPE.SSE_KMS
        ? [KMS_PROVIDER.VAULT, KMS_PROVIDER.KMIP]
        : [KMS_PROVIDER.VAULT];
  }
  checkKmsProviders() {
    if (!this.editing) {
      this.setKmsProvider();
    }

    if (
      this.allEncryptionConfigValues &&
      this.allEncryptionConfigValues.hasOwnProperty(ENCRYPTION_TYPE.SSE_KMS) &&
      !this.editing
    ) {
      const kmsBackends = Object.values(
        this.allEncryptionConfigValues[ENCRYPTION_TYPE.SSE_KMS]
      ).map((config: any) => config.backend);
      if (this.configForm.get('encryptionType').value === ENCRYPTION_TYPE.SSE_KMS) {
        this.kmsProviders = this.kmsProviders.filter((provider) => !kmsBackends.includes(provider));
      }
    }
    if (
      this.allEncryptionConfigValues &&
      this.allEncryptionConfigValues.hasOwnProperty('s3') &&
      !this.editing
    ) {
      const s3Backends = Object.values(this.allEncryptionConfigValues[ENCRYPTION_TYPE.SSE_S3]).map(
        (config: any) => config.backend
      );
      if (this.configForm.get('encryptionType').value === ENCRYPTION_TYPE.SSE_S3) {
        this.kmsProviders = this.kmsProviders.filter((provider) => !s3Backends.includes(provider));
      }
    }
    if (!this.editing) {
      if (this.kmsProviders.length > 0) {
        this.configForm.get('kms_provider').setValue(this.kmsProviders[0]);
      }
    }
  }

  createForm() {
    this.configForm = this.formBuilder.group({
      addr: [
        null,
        [
          Validators.required,
          CdValidators.custom('vaultPattern', (value: string) => {
            if (_.isEmpty(value)) {
              return false;
            }
            return !this.vaultAddress.test(value);
          })
        ]
      ],
      kms_provider: ['vault', Validators.required],
      encryptionType: ['kms', Validators.required],
      auth: [
        'token',
        CdValidators.requiredIf({
          kms_provider: 'vault'
        })
      ],
      secret_engine: [
        'kv',
        CdValidators.requiredIf({
          kms_provider: 'vault'
        })
      ],
      secret_path: ['/'],
      namespace: [null],
      token: [
        null,
        [
          CdValidators.requiredIf({
            auth: 'token',
            kms_provider: 'vault'
          })
        ]
      ],
      ssl_cert: [''],
      client_cert: [''],
      client_key: [''],
      kmsEnabled: [{ value: false }],
      s3Enabled: [{ value: false }],
      kms_key_template: [null],
      s3_key_template: [null],
      username: [''],
      password: ['']
    });
  }

  fileUpload(files: FileList, controlName: string) {
    const file: File = files[0];
    const reader = new FileReader();
    reader.addEventListener('load', () => {
      const control: AbstractControl = this.configForm.get(controlName);
      control.setValue(file);
      control.markAsDirty();
      control.markAsTouched();
      control.updateValueAndValidity();
    });
  }

  onSubmit() {
    const values = this.configForm.getRawValue();
    let encryptionData: VaultConfig | KmipConfig;
    if (values['kms_provider'] === KMS_PROVIDER.VAULT) {
      encryptionData = {
        kms_provider: values['kms_provider'],
        encryption_type: values['encryptionType'],
        config: {
          addr: values['addr'],
          auth: values['auth'],
          prefix: values['secret_path'],
          secret_engine: values['secret_engine'],
          namespace: values['namespace'],
          token_file: values['token'],
          ssl_cacert: values['ssl_cert'],
          ssl_clientcert: values['client_cert'],
          ssl_clientkey: values['client_key']
        }
      };
    } else if (values['kms_provider'] === KMS_PROVIDER.KMIP) {
      encryptionData = {
        kms_provider: values['kms_provider'],
        encryption_type: values['encryptionType'],
        config: {
          addr: values['addr'],
          username: values['username'],
          password: values['password'],
          kms_key_template: values['kms_key_template'],
          s3_key_template: values['s3_key_template'],
          client_key: values['client_key'],
          ca_path: values['ssl_cert'],
          client_cert: values['client_cert']
        }
      };
    }
    this.rgwBucketService.setEncryptionConfig(encryptionData).subscribe({
      next: () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Updated RGW Encryption Configuration values`
        );
      },
      error: (error: Error) => {
        this.notificationService.show(NotificationType.error, error.message);
        this.configForm.setErrors({ cdSubmitButton: true });
      },
      complete: () => {
        this.activeModal.close();
        this.table?.refreshBtn();
      }
    });
  }
}
