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
import { rgwBucketEncryptionModel } from '../models/rgw-bucket-encryption';
import { TableComponent } from '~/app/shared/datatable/table/table.component';

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
        address: this.selectedEncryptionConfigValues['addr'],
        encryptionType:
          rgwBucketEncryptionModel[this.selectedEncryptionConfigValues['encryption_type']],
        kms_provider: this.selectedEncryptionConfigValues['backend'],
        auth_method: this.selectedEncryptionConfigValues['auth'],
        secret_engine: this.selectedEncryptionConfigValues['secret_engine'],
        secret_path: this.selectedEncryptionConfigValues['prefix'],
        namespace: this.selectedEncryptionConfigValues['namespace']
      };
      this.configForm.patchValue(patchValues);
      this.configForm.get('kms_provider').disable();
    }
    this.checkKmsProviders();
  }

  checkKmsProviders() {
    this.kmsProviders = rgwBucketEncryptionModel.kmsProviders;
    if (
      this.allEncryptionConfigValues &&
      this.allEncryptionConfigValues.hasOwnProperty('SSE_KMS') &&
      !this.editing
    ) {
      const sseKmsBackends = this.allEncryptionConfigValues['SSE_KMS'].map(
        (config: any) => config.backend
      );
      if (this.configForm.get('encryptionType').value === rgwBucketEncryptionModel.SSE_KMS) {
        this.kmsProviders = this.kmsProviders.filter(
          (provider) => !sseKmsBackends.includes(provider)
        );
      }
    }
    if (
      this.allEncryptionConfigValues &&
      this.allEncryptionConfigValues.hasOwnProperty('SSE_S3') &&
      !this.editing
    ) {
      const sseS3Backends = this.allEncryptionConfigValues['SSE_S3'].map(
        (config: any) => config.backend
      );
      if (this.configForm.get('encryptionType').value === rgwBucketEncryptionModel.SSE_S3) {
        this.kmsProviders = this.kmsProviders.filter(
          (provider) => !sseS3Backends.includes(provider)
        );
      }
    }
    if (this.kmsProviders.length > 0 && !this.kmsProviders.includes('vault')) {
      this.configForm.get('kms_provider').setValue('');
    }
  }

  createForm() {
    this.configForm = this.formBuilder.group({
      address: [
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
      encryptionType: ['aws:kms', Validators.required],
      auth_method: ['token', Validators.required],
      secret_engine: ['kv', Validators.required],
      secret_path: ['/'],
      namespace: [null],
      token: [
        null,
        [
          CdValidators.requiredIf({
            auth_method: 'token'
          })
        ]
      ],
      ssl_cert: [null, CdValidators.sslCert()],
      client_cert: [null, CdValidators.pemCert()],
      client_key: [null, CdValidators.sslPrivKey()],
      kmsEnabled: [{ value: false }],
      s3Enabled: [{ value: false }]
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
    this.rgwBucketService
      .setEncryptionConfig(
        values['encryptionType'],
        values['kms_provider'],
        values['auth_method'],
        values['secret_engine'],
        values['secret_path'],
        values['namespace'],
        values['address'],
        values['token'],
        values['owner'],
        values['ssl_cert'],
        values['client_cert'],
        values['client_key']
      )
      .subscribe({
        next: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Updated RGW Encryption Configuration values`
          );
        },
        error: (error: any) => {
          this.notificationService.show(NotificationType.error, error);
          this.configForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.activeModal.close();
          this.table?.refreshBtn();
        }
      });
  }
}
