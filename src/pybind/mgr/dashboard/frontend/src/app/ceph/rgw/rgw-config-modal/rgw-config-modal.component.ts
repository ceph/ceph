import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { AbstractControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwBucketEncryptionModel } from '../models/rgw-bucket-encryption';

@Component({
  selector: 'cd-rgw-config-modal',
  templateUrl: './rgw-config-modal.component.html',
  styleUrls: ['./rgw-config-modal.component.scss'],
  providers: [RgwBucketEncryptionModel]
})
export class RgwConfigModalComponent implements OnInit {
  readonly vaultAddress = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{4}$/;

  kmsProviders: string[];

  configForm: CdFormGroup;

  @Output()
  submitAction = new EventEmitter();
  authMethods: string[];
  secretEngines: string[];

  encryptionConfigValues: any = {};
  editing = false;

  constructor(
    private formBuilder: CdFormBuilder,
    public activeModal: NgbActiveModal,
    private router: Router,
    public actionLabels: ActionLabelsI18n,
    private rgwBucketService: RgwBucketService,
    private rgwEncryptionModal: RgwBucketEncryptionModel,
    private notificationService: NotificationService
  ) {
    this.createForm();
  }
  ngOnInit(): void {
    this.kmsProviders = this.rgwEncryptionModal.kmsProviders;
    this.authMethods = this.rgwEncryptionModal.authMethods;
    this.secretEngines = this.rgwEncryptionModal.secretEngines;
    if (this.editing && this.encryptionConfigValues) {
      this.configForm.get('address').setValue(this.encryptionConfigValues['addr']);
      if (this.encryptionConfigValues['encryption_type'] === 'SSE-KMS Encryption') {
        this.configForm.get('encryptionType').setValue('aws:kms');
      } else if (this.encryptionConfigValues['encryption_type'] === 'SSE-S3 Encryption') {
        this.configForm.get('encryptionType').setValue('AES256');
      }
      this.configForm.get('kms_provider').setValue(this.encryptionConfigValues['backend']);
      this.configForm.get('auth_method').setValue(this.encryptionConfigValues['auth']);
      this.configForm.get('secret_engine').setValue(this.encryptionConfigValues['secret_engine']);
      this.configForm.get('secret_path').setValue(this.encryptionConfigValues['prefix']);
      this.configForm.get('namespace').setValue(this.encryptionConfigValues['namespace']);
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
    const values = this.configForm.value;
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
          this.router.routeReuseStrategy.shouldReuseRoute = () => false;
          this.router.onSameUrlNavigation = 'reload';
          this.router.navigate([this.router.url]);
        }
      });
  }
}
