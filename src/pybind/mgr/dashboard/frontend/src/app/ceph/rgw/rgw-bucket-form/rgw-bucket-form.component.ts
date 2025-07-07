import {
  AfterViewChecked,
  ChangeDetectorRef,
  Component,
  OnInit,
  ViewChild,
  ElementRef
} from '@angular/core';
import { AbstractControl, FormGroup, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import _ from 'lodash';
import { Observable, forkJoin } from 'rxjs';
import * as xml2js from 'xml2js';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwSiteService } from '~/app/shared/api/rgw-site.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { ActionLabelsI18n, AppConstants, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { rgwBucketEncryptionModel } from '../models/rgw-bucket-encryption';
import { RgwBucketMfaDelete } from '../models/rgw-bucket-mfa-delete';
import {
  AclPermissionsType,
  RgwBucketAclPermissions as aclPermission,
  RgwBucketAclGrantee as Grantee
} from './rgw-bucket-acl-permissions.enum';
import { RgwBucketVersioning } from '../models/rgw-bucket-versioning';
import { BucketTagModalComponent } from '../bucket-tag-modal/bucket-tag-modal.component';
import { TextAreaJsonFormatterService } from '~/app/shared/services/text-area-json-formatter.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { map, switchMap } from 'rxjs/operators';
import { TextAreaXmlFormatterService } from '~/app/shared/services/text-area-xml-formatter.service';
import { RgwRateLimitComponent } from '../rgw-rate-limit/rgw-rate-limit.component';
import { RgwRateLimitConfig } from '../models/rgw-rate-limit';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';
import { RgwUser } from '../models/rgw-user';

@Component({
  selector: 'cd-rgw-bucket-form',
  templateUrl: './rgw-bucket-form.component.html',
  styleUrls: ['./rgw-bucket-form.component.scss']
})
export class RgwBucketFormComponent extends CdForm implements OnInit, AfterViewChecked {
  @ViewChild('bucketPolicyTextArea')
  public bucketPolicyTextArea: ElementRef<any>;
  @ViewChild('lifecycleTextArea')
  public lifecycleTextArea: ElementRef<any>;

  bucketForm: CdFormGroup;
  editing = false;
  owners: RgwUser[] = null;
  accounts: string[] = [];
  accountUsers: RgwUser[] = [];
  kmsProviders: string[] = null;
  action: string;
  resource: string;
  zonegroup: string;
  placementTargets: object[] = [];
  isVersioningAlreadyEnabled = false;
  isMfaDeleteAlreadyEnabled = false;
  icons = Icons;
  kmsConfigured = false;
  s3Configured = false;
  tags: Record<string, string>[] = [];
  dirtyTags = false;
  tagConfig = [
    {
      attribute: 'key'
    },
    {
      attribute: 'value'
    }
  ];
  grantees: string[] = [Grantee.Owner, Grantee.Everyone, Grantee.AuthenticatedUsers];
  aclPermissions: AclPermissionsType[] = [aclPermission.FullControl];
  multisiteStatus$: Observable<any>;
  isDefaultZoneGroup$: Observable<boolean>;

  get isVersioningEnabled(): boolean {
    return this.bucketForm.getValue('versioning');
  }
  get isMfaDeleteEnabled(): boolean {
    return this.bucketForm.getValue('mfa-delete');
  }

  @ViewChild(RgwRateLimitComponent, { static: false }) rateLimitComponent!: RgwRateLimitComponent;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private formBuilder: CdFormBuilder,
    private rgwBucketService: RgwBucketService,
    private rgwSiteService: RgwSiteService,
    private modalService: ModalCdsService,
    private rgwUserService: RgwUserService,
    private notificationService: NotificationService,
    private textAreaJsonFormatterService: TextAreaJsonFormatterService,
    private textAreaXmlFormatterService: TextAreaXmlFormatterService,
    public actionLabels: ActionLabelsI18n,
    private readonly changeDetectorRef: ChangeDetectorRef,
    private rgwMultisiteService: RgwMultisiteService,
    private rgwDaemonService: RgwDaemonService,
    private rgwAccountsService: RgwUserAccountsService
  ) {
    super();
    this.editing = this.router.url.startsWith(`/rgw/bucket/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = $localize`bucket`;
    this.createForm();
  }

  ngAfterViewChecked(): void {
    this.changeDetectorRef.detectChanges();
    this.textAreaOnChange(this.bucketPolicyTextArea);
    this.textAreaOnChange(this.lifecycleTextArea);
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
      owner: [
        null,
        [
          CdValidators.requiredIf({
            isAccountOwner: false
          })
        ]
      ],
      kms_provider: ['vault'],
      'placement-target': [null],
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
      lock_retention_period_days: [10, [CdValidators.number(false), lockDaysValidator]],
      bucket_policy: ['{}', CdValidators.json()],
      lifecycle: ['{}', CdValidators.jsonOrXml()],
      grantee: [Grantee.Owner, [Validators.required]],
      aclPermission: [[aclPermission.FullControl], [Validators.required]],
      replication: [false],
      isAccountOwner: [false],
      accountUser: [
        null,
        [
          CdValidators.requiredIf({
            isAccountOwner: true
          })
        ]
      ]
    });
  }

  ngOnInit() {
    const promises = {
      owners: this.rgwUserService.enumerate(true),
      accounts: this.rgwAccountsService.list()
    };
    this.multisiteStatus$ = this.rgwMultisiteService.status();
    this.isDefaultZoneGroup$ = this.rgwDaemonService.selectedDaemon$.pipe(
      switchMap((daemon) =>
        this.rgwSiteService.get('default-zonegroup').pipe(
          map((defaultZoneGroup) => {
            return daemon.zonegroup_id === defaultZoneGroup;
          })
        )
      )
    );

    this.kmsProviders = rgwBucketEncryptionModel.kmsProviders;
    this.rgwBucketService.getEncryptionConfig().subscribe((data) => {
      this.s3Configured = data.s3 && Object.keys(data.s3).length > 0;
      this.kmsConfigured = data.kms && Object.keys(data.kms).length > 0;
      // Set the encryption type based on the configurations
      if (this.kmsConfigured && this.s3Configured) {
        this.bucketForm.get('encryption_type').setValue('');
      } else if (this.kmsConfigured) {
        this.bucketForm.get('encryption_type').setValue('aws:kms');
      } else if (this.s3Configured) {
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
        this.accounts = data.accounts;
        this.accountUsers = data.owners.filter((owner: RgwUser) => owner.account_id);
        this.owners = data.owners.filter((owner: RgwUser) => !owner.account_id);
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
          if (this.editing) {
            [value['grantee'], value['aclPermission']] = this.aclXmlToFormValues(
              bidResp['acl'],
              bidResp['owner']
            );
            value['lifecycle'] = JSON.stringify(bidResp['lifecycle'] || {});
          }
          this.bucketForm.setValue(value);
          if (this.editing) {
            // Disable changing the owner of the bucket in case
            // its owned by the account.
            const ownersList = data.owners.map((owner: RgwUser) => owner.uid);
            if (!ownersList.includes(value['owner'])) {
              // creating dummy user object to show the account owner
              // here value['owner] is the account user id
              const user = Object.assign(
                { uid: value['owner'] },
                ownersList.find((owner: RgwUser) => owner.uid === AppConstants.defaultUser)
              );
              this.accountUsers.push(user);
              this.bucketForm.get('isAccountOwner').setValue(true);
              this.bucketForm.get('isAccountOwner').disable();
              this.bucketForm.get('accountUser').setValue(value['owner']);
              this.bucketForm.get('accountUser').disable();
            }
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
            if (value['replication']) {
              const replicationConfig = value['replication'];
              if (replicationConfig?.['Rule']?.['Status'] === 'Enabled') {
                this.bucketForm.get('replication').setValue(true);
              } else {
                this.bucketForm.get('replication').setValue(false);
              }
            }
            this.filterAclPermissions();
          }
        }
        this.loadingReady();
      });
    });
  }

  goToListView() {
    this.router.navigate(['/rgw/bucket']);
  }
  rateLimitFormInit(rateLimitForm: FormGroup) {
    this.bucketForm.addControl('rateLimit', rateLimitForm);
  }
  submit() {
    // Exit immediately if the form isn't dirty.
    if (this.bucketForm.pristine && this.rateLimitComponent.form.pristine) {
      this.goToListView();
      return;
    }

    // Ensure that no validation is pending
    if (this.bucketForm.pending) {
      this.bucketForm.setErrors({ cdSubmitButton: true });
      return;
    }

    if (this.bucketForm.getValue('encryption_enabled') == null) {
      this.bucketForm.get('encryption_enabled').setValue(false);
      this.bucketForm.get('encryption_type').setValue(null);
    }

    const values = this.bucketForm.value;
    const xmlStrTags = this.tagsToXML(this.tags);
    const bucketPolicy = this.getBucketPolicy();
    const cannedAcl = this.permissionToCannedAcl();

    if (this.editing) {
      // Edit
      const versioning = this.getVersioningStatus();
      const mfaDelete = this.getMfaDeleteStatus();
      // make the owner empty if the field is disabled.
      // this ensures the bucket doesn't gets updated with owner when
      // the bucket is owned by the account.
      let owner;
      if (this.bucketForm.get('accountUser').disabled) {
        // If the bucket is owned by the account, then set the owner as account user.
        owner = '';
      } else if (values['isAccountOwner']) {
        const accountUser: RgwUser = this.accountUsers.filter(
          (user) => values['accountUser'] === user.uid
        )[0];
        owner = accountUser?.account_id ?? values['owner'];
      } else {
        owner = values['owner'];
      }

      this.rgwBucketService
        .update(
          values['bid'],
          values['id'],
          owner,
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
          bucketPolicy,
          cannedAcl,
          values['replication'],
          values['lifecycle']
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Updated Object Gateway bucket '${values.bid}'.`
            );
            this.updateBucketRateLimit();
            this.goToListView();
          },
          () => {
            // Reset the 'Submit' button.
            this.bucketForm.setErrors({ cdSubmitButton: true });
          }
        );
    } else {
      const owner = values['isAccountOwner'] ? values['accountUser'] : values['owner'];
      // Add
      this.rgwBucketService
        .create(
          values['bid'],
          owner,
          this.zonegroup,
          values['placement-target'],
          values['lock_enabled'],
          values['lock_mode'],
          values['lock_retention_period_days'],
          values['encryption_enabled'],
          values['encryption_type'],
          values['keyId'],
          xmlStrTags,
          bucketPolicy,
          cannedAcl,
          values['replication']
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Created Object Gateway bucket '${values.bid}'`
            );
            this.goToListView();
            this.updateBucketRateLimit();
          },
          () => {
            // Reset the 'Submit' button.
            this.bucketForm.setErrors({ cdSubmitButton: true });
          }
        );
    }
  }

  updateBucketRateLimit() {
    /**
     * Whenever we change the owner of bucket from non-tenanted to tenanted
     * and vice-versa with the rate-limit changes there was issue in sending
     * bucket name, hence the below logic caters to it.
     *
     * Scenario 1: Changing the bucket owner from tenanted to non-tenanted
     * Scenario 2: Changing the bucket owner from non-tenanted to tenanted
     * Scenario 3: Keeping the owner(tenanted) same and changing only rate limit
     */
    // in case of creating bucket with account user, owner will be empty
    const owner = this.bucketForm.getValue('owner') || '';
    const bidInput = this.bucketForm.getValue('bid');

    let bid: string;

    const hasOwnerWithDollar = owner.includes('$');
    const bidHasSlash = bidInput.includes('/');

    if (bidHasSlash && hasOwnerWithDollar) {
      bid = bidInput;
    } else if (hasOwnerWithDollar) {
      const ownerPrefix = owner.split('$')[0];
      bid = `${ownerPrefix}/${bidInput}`;
    } else if (bidHasSlash) {
      bid = bidInput.split('/')[1];
    } else {
      bid = bidInput;
    }
    // Check if bucket ratelimit has been modified.
    const rateLimitConfig: RgwRateLimitConfig = this.rateLimitComponent.getRateLimitFormValue();
    if (!!rateLimitConfig) {
      this.rgwBucketService.updateBucketRateLimit(bid, rateLimitConfig).subscribe(
        () => {},
        (error: any) => {
          this.notificationService.show(NotificationType.error, error);
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

  textAreaOnChange(textArea: ElementRef<any>) {
    if (textArea?.nativeElement?.value?.startsWith?.('<')) {
      this.textAreaXmlFormatterService.format(textArea);
    } else {
      this.textAreaJsonFormatterService.format(textArea);
    }
  }

  openUrl(url: string) {
    window.open(url, '_blank');
  }

  clearTextArea(field: string, defaultValue: string = '') {
    this.bucketForm.get(field).setValue(defaultValue);
    this.bucketForm.markAsDirty();
    this.bucketForm.updateValueAndValidity();
  }

  showTagModal(index?: number) {
    const modalRef = this.modalService.show(BucketTagModalComponent);
    const modalComponent = modalRef as BucketTagModalComponent;
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
    this.dirtyTags = true;
    this.bucketForm.markAsDirty();
    this.bucketForm.updateValueAndValidity();
  }

  private setTag(tag: Record<string, string>, index?: number) {
    if (_.isNumber(index)) {
      this.tags[index] = tag;
    } else {
      this.tags.push(tag);
    }
    this.dirtyTags = true;
    this.bucketForm.markAsDirty();
    this.bucketForm.updateValueAndValidity();
  }

  private tagsToXML(tags: Record<string, string>[]): string {
    if (!this.dirtyTags && tags.length === 0) return '';
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

  aclXmlToFormValues(xml: any, bucketOwner: string): [Grantee, AclPermissionsType] {
    const parser = new xml2js.Parser({ explicitArray: false, trim: true });
    let selectedAclPermission: AclPermissionsType = aclPermission.FullControl;
    let selectedGrantee: Grantee = Grantee.Owner;
    parser.parseString(xml, (err, result) => {
      if (err) return null;

      const xmlGrantees: any = result['AccessControlPolicy']['AccessControlList']['Grant'];
      for (let i = 0; i < xmlGrantees.length; i++) {
        if (xmlGrantees[i]['Grantee']['ID'] === bucketOwner) continue;
        if (
          xmlGrantees[i]['Grantee']['URI'] &&
          xmlGrantees[i]['Grantee']['URI'].includes('AllUsers')
        ) {
          selectedGrantee = Grantee.Everyone;
          if (
            xmlGrantees[i]['Permission'] === 'READ' &&
            selectedAclPermission !== aclPermission.Write
          ) {
            selectedAclPermission = aclPermission.Read;
          } else if (
            xmlGrantees[i]['Permission'] === ' WRITE' &&
            selectedAclPermission !== aclPermission.Read
          ) {
            selectedAclPermission = aclPermission.Write;
          } else {
            selectedAclPermission = aclPermission.All;
          }
        } else if (
          xmlGrantees[i]['Grantee']['URI'] &&
          xmlGrantees[i]['Grantee']['URI'].includes('AuthenticatedUsers')
        ) {
          selectedGrantee = Grantee.AuthenticatedUsers;
          selectedAclPermission = aclPermission.Read;
        }
      }
    });
    return [selectedGrantee, selectedAclPermission];
  }

  /*
   Set the selector's options to the available options depending
   on the selected Grantee and reset it's value
   */
  onSelectionFilter() {
    this.filterAclPermissions();
    this.bucketForm.get('aclPermission').setValue(this.aclPermissions[0]);
  }

  filterAclPermissions() {
    const selectedGrantee: Grantee = this.bucketForm.get('grantee').value;
    switch (selectedGrantee) {
      case Grantee.Owner:
        this.aclPermissions = [aclPermission.FullControl];
        break;
      case Grantee.Everyone:
        this.aclPermissions = [aclPermission.Read, aclPermission.All];
        break;
      case Grantee.AuthenticatedUsers:
        this.aclPermissions = [aclPermission.Read];
        break;
    }
  }

  permissionToCannedAcl(): string {
    const selectedGrantee: Grantee = this.bucketForm.get('grantee').value;
    const selectedAclPermission = this.bucketForm.get('aclPermission').value;
    switch (selectedGrantee) {
      case Grantee.Everyone:
        return selectedAclPermission === aclPermission.Read ? 'public-read' : 'public-read-write';
      case Grantee.AuthenticatedUsers:
        return 'authenticated-read';
      default:
        return 'private';
    }
  }
}
