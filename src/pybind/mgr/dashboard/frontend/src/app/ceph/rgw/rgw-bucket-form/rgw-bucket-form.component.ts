import {
  AfterViewChecked,
  ChangeDetectorRef,
  Component,
  OnInit,
  ViewChild,
  ElementRef
} from '@angular/core';
import { AbstractControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import _ from 'lodash';
import { forkJoin } from 'rxjs';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwSiteService } from '~/app/shared/api/rgw-site.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwBucketEncryptionModel } from '../models/rgw-bucket-encryption';
import { RgwBucketMfaDelete } from '../models/rgw-bucket-mfa-delete';
import { RgwBucketVersioning } from '../models/rgw-bucket-versioning';
import { RgwConfigModalComponent } from '../rgw-config-modal/rgw-config-modal.component';
import { BucketTagModalComponent } from '../bucket-tag-modal/bucket-tag-modal.component';
import { TextAreaJsonFormatterService } from '~/app/shared/services/text-area-json-formatter.service';

@Component({
  selector: 'cd-rgw-bucket-form',
  templateUrl: './rgw-bucket-form.component.html',
  styleUrls: ['./rgw-bucket-form.component.scss'],
  providers: [RgwBucketEncryptionModel]
})
export class RgwBucketFormComponent extends CdForm implements OnInit, AfterViewChecked {
  @ViewChild('bucketPolicyTextArea')
  public bucketPolicyTextArea: ElementRef<any>;

  bucketForm: CdFormGroup;
  editing = false;
  owners: string[] = null;
  kmsProviders: string[] = null;
  action: string;
  resource: string;
  zonegroup: string;
  placementTargets: object[] = [];
  isVersioningAlreadyEnabled = false;
  isMfaDeleteAlreadyEnabled = false;
  icons = Icons;
  kmsVaultConfig = false;
  s3VaultConfig = false;
  tags: Record<string, string>[] = [];
  tagConfig = [
    {
      attribute: 'key'
    },
    {
      attribute: 'value'
    }
  ];

  get isVersioningEnabled(): boolean {
    return this.bucketForm.getValue('versioning');
  }
  get isMfaDeleteEnabled(): boolean {
    return this.bucketForm.getValue('mfa-delete');
  }

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private formBuilder: CdFormBuilder,
    private rgwBucketService: RgwBucketService,
    private rgwSiteService: RgwSiteService,
    private modalService: ModalService,
    private rgwUserService: RgwUserService,
    private notificationService: NotificationService,
    private rgwEncryptionModal: RgwBucketEncryptionModel,
    private textAreaJsonFormatterService: TextAreaJsonFormatterService,
    public actionLabels: ActionLabelsI18n,
    private readonly changeDetectorRef: ChangeDetectorRef
  ) {
    super();
    this.editing = this.router.url.startsWith(`/rgw/bucket/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = $localize`bucket`;
    this.createForm();
  }

  ngAfterViewChecked(): void {
    this.changeDetectorRef.detectChanges();
    this.bucketPolicyOnChange();
  }

  createForm() {
    const self = this;
    const lockDaysValidator = CdValidators.custom('lockDays', () => {
      if (!self.bucketForm || !_.get(self.bucketForm.getRawValue(), 'lock_enabled')) {
        return false;
      }
      const lockDays = Number(self.bucketForm.getValue('lock_retention_period_days'));
      return !Number.isInteger(lockDays) || lockDays === 0;
    });
    this.bucketForm = this.formBuilder.group({
      id: [null],
      bid: [
        null,
        [Validators.required],
        this.editing
          ? []
          : [CdValidators.bucketName(), CdValidators.bucketExistence(false, this.rgwBucketService)]
      ],
      owner: [null, [Validators.required]],
      kms_provider: ['vault'],
      'placement-target': [null, this.editing ? [] : [Validators.required]],
      versioning: [null],
      'mfa-delete': [null],
      'mfa-token-serial': [''],
      'mfa-token-pin': [''],
      lock_enabled: [{ value: false, disabled: this.editing }],
      encryption_enabled: [null],
      encryption_type: [
        null,
        [
          CdValidators.requiredIf({
            encryption_enabled: true
          })
        ]
      ],
      keyId: [
        null,
        [
          CdValidators.requiredIf({
            encryption_type: 'aws:kms',
            encryption_enabled: true
          })
        ]
      ],
      lock_mode: ['COMPLIANCE'],
      lock_retention_period_days: [0, [CdValidators.number(false), lockDaysValidator]],
      bucket_policy: ['{}', CdValidators.json()]
    });
  }

  ngOnInit() {
    const promises = {
      owners: this.rgwUserService.enumerate()
    };

    this.kmsProviders = this.rgwEncryptionModal.kmsProviders;
    this.rgwBucketService.getEncryptionConfig().subscribe((data) => {
      this.kmsVaultConfig = data[0];
      this.s3VaultConfig = data[1];
      if (this.kmsVaultConfig && this.s3VaultConfig) {
        this.bucketForm.get('encryption_type').setValue('');
      } else if (this.kmsVaultConfig) {
        this.bucketForm.get('encryption_type').setValue('aws:kms');
      } else if (this.s3VaultConfig) {
        this.bucketForm.get('encryption_type').setValue('AES256');
      } else {
        this.bucketForm.get('encryption_type').setValue('');
      }
    });

    if (!this.editing) {
      promises['getPlacementTargets'] = this.rgwSiteService.get('placement-targets');
    }

    // Process route parameters.
    this.route.params.subscribe((params: { bid: string }) => {
      if (params.hasOwnProperty('bid')) {
        const bid = decodeURIComponent(params.bid);
        promises['getBid'] = this.rgwBucketService.get(bid);
      }

      forkJoin(promises).subscribe((data: any) => {
        // Get the list of possible owners.
        this.owners = (<string[]>data.owners).sort();

        // Get placement targets:
        if (data['getPlacementTargets']) {
          const placementTargets = data['getPlacementTargets'];
          this.zonegroup = placementTargets['zonegroup'];
          _.forEach(placementTargets['placement_targets'], (placementTarget) => {
            placementTarget['description'] = `${placementTarget['name']} (${$localize`pool`}: ${
              placementTarget['data_pool']
            })`;
            this.placementTargets.push(placementTarget);
          });

          // If there is only 1 placement target, select it by default:
          if (this.placementTargets.length === 1) {
            this.bucketForm.get('placement-target').setValue(this.placementTargets[0]['name']);
          }
        }

        if (data['getBid']) {
          const bidResp = data['getBid'];
          // Get the default values (incl. the values from disabled fields).
          const defaults = _.clone(this.bucketForm.getRawValue());

          // Get the values displayed in the form. We need to do that to
          // extract those key/value pairs from the response data, otherwise
          // the Angular react framework will throw an error if there is no
          // field for a given key.
          let value: object = _.pick(bidResp, _.keys(defaults));

          value['lock_retention_period_days'] = this.rgwBucketService.getLockDays(bidResp);
          value['placement-target'] = bidResp['placement_rule'];
          value['versioning'] = bidResp['versioning'] === RgwBucketVersioning.ENABLED;
          value['mfa-delete'] = bidResp['mfa_delete'] === RgwBucketMfaDelete.ENABLED;
          value['encryption_enabled'] = bidResp['encryption'] === 'Enabled';
          if (bidResp['tagset']) {
            for (const [key, value] of Object.entries(bidResp['tagset'])) {
              this.tags.push({ key: key, value: value.toString() });
            }
          }
          // Append default values.
          value = _.merge(defaults, value);
          // Update the form.
          this.bucketForm.setValue(value);
          if (this.editing) {
            this.isVersioningAlreadyEnabled = this.isVersioningEnabled;
            this.isMfaDeleteAlreadyEnabled = this.isMfaDeleteEnabled;
            this.setMfaDeleteValidators();
            if (value['lock_enabled']) {
              this.bucketForm.controls['versioning'].disable();
            }
            if (value['bucket_policy']) {
              this.bucketForm
                .get('bucket_policy')
                .setValue(JSON.stringify(value['bucket_policy'], null, 2));
            }
          }
        }
        this.loadingReady();
      });
    });
  }

  goToListView() {
    this.router.navigate(['/rgw/bucket']);
  }

  submit() {
    // Exit immediately if the form isn't dirty.
    if (this.bucketForm.getValue('encryption_enabled') == null) {
      this.bucketForm.get('encryption_enabled').setValue(false);
      this.bucketForm.get('encryption_type').setValue(null);
    }
    if (this.bucketForm.pristine) {
      this.goToListView();
      return;
    }
    const values = this.bucketForm.value;
    const xmlStrTags = this.tagsToXML(this.tags);
    const bucketPolicy = this.getBucketPolicy();
    if (this.editing) {
      // Edit
      const versioning = this.getVersioningStatus();
      const mfaDelete = this.getMfaDeleteStatus();
      this.rgwBucketService
        .update(
          values['bid'],
          values['id'],
          values['owner'],
          versioning,
          values['encryption_enabled'],
          values['encryption_type'],
          values['keyId'],
          mfaDelete,
          values['mfa-token-serial'],
          values['mfa-token-pin'],
          values['lock_mode'],
          values['lock_retention_period_days'],
          xmlStrTags,
          bucketPolicy
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Updated Object Gateway bucket '${values.bid}'.`
            );
            this.goToListView();
          },
          () => {
            // Reset the 'Submit' button.
            this.bucketForm.setErrors({ cdSubmitButton: true });
          }
        );
    } else {
      // Add
      this.rgwBucketService
        .create(
          values['bid'],
          values['owner'],
          this.zonegroup,
          values['placement-target'],
          values['lock_enabled'],
          values['lock_mode'],
          values['lock_retention_period_days'],
          values['encryption_enabled'],
          values['encryption_type'],
          values['keyId'],
          xmlStrTags,
          bucketPolicy
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Created Object Gateway bucket '${values.bid}'`
            );
            this.goToListView();
          },
          () => {
            // Reset the 'Submit' button.
            this.bucketForm.setErrors({ cdSubmitButton: true });
          }
        );
    }
  }

  areMfaCredentialsRequired() {
    return (
      this.isMfaDeleteEnabled !== this.isMfaDeleteAlreadyEnabled ||
      (this.isMfaDeleteAlreadyEnabled &&
        this.isVersioningEnabled !== this.isVersioningAlreadyEnabled)
    );
  }

  setMfaDeleteValidators() {
    const mfaTokenSerialControl = this.bucketForm.get('mfa-token-serial');
    const mfaTokenPinControl = this.bucketForm.get('mfa-token-pin');

    if (this.areMfaCredentialsRequired()) {
      mfaTokenSerialControl.setValidators(Validators.required);
      mfaTokenPinControl.setValidators(Validators.required);
    } else {
      mfaTokenSerialControl.setValidators(null);
      mfaTokenPinControl.setValidators(null);
    }

    mfaTokenSerialControl.updateValueAndValidity();
    mfaTokenPinControl.updateValueAndValidity();
  }

  getVersioningStatus() {
    return this.isVersioningEnabled ? RgwBucketVersioning.ENABLED : RgwBucketVersioning.SUSPENDED;
  }

  getMfaDeleteStatus() {
    return this.isMfaDeleteEnabled ? RgwBucketMfaDelete.ENABLED : RgwBucketMfaDelete.DISABLED;
  }

  getBucketPolicy() {
    return this.bucketForm.getValue('bucket_policy') || '{}';
  }

  fileUpload(files: FileList, controlName: string) {
    const file: File = files[0];
    const reader = new FileReader();
    reader.addEventListener('load', () => {
      const control: AbstractControl = this.bucketForm.get(controlName);
      control.setValue(file);
      control.markAsDirty();
      control.markAsTouched();
      control.updateValueAndValidity();
    });
  }

  bucketPolicyOnChange() {
    if (this.bucketPolicyTextArea) {
      this.textAreaJsonFormatterService.format(this.bucketPolicyTextArea);
    }
  }

  openUrl(url: string) {
    window.open(url, '_blank');
  }

  clearBucketPolicy() {
    this.bucketForm.get('bucket_policy').setValue('{}');
    this.bucketForm.markAsDirty();
    this.bucketForm.updateValueAndValidity();
  }

  openConfigModal() {
    const modalRef = this.modalService.show(RgwConfigModalComponent, null, { size: 'lg' });
    modalRef.componentInstance.configForm
      .get('encryptionType')
      .setValue(this.bucketForm.getValue('encryption_type') || 'AES256');
  }

  showTagModal(index?: number) {
    const modalRef = this.modalService.show(BucketTagModalComponent);
    const modalComponent = modalRef.componentInstance as BucketTagModalComponent;
    modalComponent.currentKeyTags = this.tags.map((item) => item.key);

    if (_.isNumber(index)) {
      modalComponent.editMode = true;
      modalComponent.fillForm(this.tags[index]);
      modalComponent.storedKey = this.tags[index]['key'];
    }

    modalComponent.submitAction.subscribe((tag: Record<string, string>) => {
      this.setTag(tag, index);
    });
  }

  deleteTag(index: number) {
    this.tags.splice(index, 1);
    this.bucketForm.markAsDirty();
    this.bucketForm.updateValueAndValidity();
  }

  private setTag(tag: Record<string, string>, index?: number) {
    if (_.isNumber(index)) {
      this.tags[index] = tag;
    } else {
      this.tags.push(tag);
    }
    this.bucketForm.markAsDirty();
    this.bucketForm.updateValueAndValidity();
  }

  private tagsToXML(tags: Record<string, string>[]): string {
    let xml = '<Tagging><TagSet>';
    for (const tag of tags) {
      xml += '<Tag>';
      for (const key in tag) {
        if (key === 'key') {
          xml += `<Key>${tag[key]}</Key>`;
        } else if (key === 'value') {
          xml += `<Value>${tag[key]}</Value>`;
        }
      }
      xml += '</Tag>';
    }
    xml += '</TagSet></Tagging>';
    return xml;
  }
}
