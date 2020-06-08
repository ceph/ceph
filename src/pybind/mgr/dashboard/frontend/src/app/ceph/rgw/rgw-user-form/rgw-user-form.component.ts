import { Component, OnInit } from '@angular/core';
import { AbstractControl, ValidationErrors, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalService } from 'ngx-bootstrap/modal';
import { concat as observableConcat, forkJoin as observableForkJoin, Observable } from 'rxjs';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { ActionLabelsI18n, URLVerbs } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdForm } from '../../../shared/forms/cd-form';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators, isEmptyInputValue } from '../../../shared/forms/cd-validators';
import { FormatterService } from '../../../shared/services/formatter.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { RgwUserCapabilities } from '../models/rgw-user-capabilities';
import { RgwUserCapability } from '../models/rgw-user-capability';
import { RgwUserS3Key } from '../models/rgw-user-s3-key';
import { RgwUserSubuser } from '../models/rgw-user-subuser';
import { RgwUserSwiftKey } from '../models/rgw-user-swift-key';
import { RgwUserCapabilityModalComponent } from '../rgw-user-capability-modal/rgw-user-capability-modal.component';
import { RgwUserS3KeyModalComponent } from '../rgw-user-s3-key-modal/rgw-user-s3-key-modal.component';
import { RgwUserSubuserModalComponent } from '../rgw-user-subuser-modal/rgw-user-subuser-modal.component';
import { RgwUserSwiftKeyModalComponent } from '../rgw-user-swift-key-modal/rgw-user-swift-key-modal.component';

@Component({
  selector: 'cd-rgw-user-form',
  templateUrl: './rgw-user-form.component.html',
  styleUrls: ['./rgw-user-form.component.scss']
})
export class RgwUserFormComponent extends CdForm implements OnInit {
  userForm: CdFormGroup;
  editing = false;
  submitObservables: Observable<Object>[] = [];
  icons = Icons;
  subusers: RgwUserSubuser[] = [];
  s3Keys: RgwUserS3Key[] = [];
  swiftKeys: RgwUserSwiftKey[] = [];
  capabilities: RgwUserCapability[] = [];

  action: string;
  resource: string;
  subuserLabel: string;
  s3keyLabel: string;
  capabilityLabel: string;

  constructor(
    private formBuilder: CdFormBuilder,
    private route: ActivatedRoute,
    private router: Router,
    private rgwUserService: RgwUserService,
    private bsModalService: BsModalService,
    private notificationService: NotificationService,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.resource = this.i18n('user');
    this.subuserLabel = this.i18n('subuser');
    this.s3keyLabel = this.i18n('S3 Key');
    this.capabilityLabel = this.i18n('capability');
    this.createForm();
  }

  createForm() {
    this.userForm = this.formBuilder.group({
      // General
      uid: [
        null,
        [Validators.required],
        [CdValidators.unique(this.rgwUserService.exists, this.rgwUserService)]
      ],
      display_name: [null, [Validators.required]],
      email: [
        null,
        [CdValidators.email],
        [CdValidators.unique(this.rgwUserService.emailExists, this.rgwUserService)]
      ],
      max_buckets_mode: [1],
      max_buckets: [
        1000,
        [
          CdValidators.requiredIf({ max_buckets_mode: '1' }),
          CdValidators.number(false),
          Validators.min(1)
        ]
      ],
      suspended: [false],
      // S3 key
      generate_key: [true],
      access_key: [null, [CdValidators.requiredIf({ generate_key: false })]],
      secret_key: [null, [CdValidators.requiredIf({ generate_key: false })]],
      // User quota
      user_quota_enabled: [false],
      user_quota_max_size_unlimited: [true],
      user_quota_max_size: [
        null,
        [
          CdValidators.composeIf(
            {
              user_quota_enabled: true,
              user_quota_max_size_unlimited: false
            },
            [Validators.required, this.quotaMaxSizeValidator]
          )
        ]
      ],
      user_quota_max_objects_unlimited: [true],
      user_quota_max_objects: [
        null,
        [
          Validators.min(0),
          CdValidators.requiredIf({
            user_quota_enabled: true,
            user_quota_max_objects_unlimited: false
          })
        ]
      ],
      // Bucket quota
      bucket_quota_enabled: [false],
      bucket_quota_max_size_unlimited: [true],
      bucket_quota_max_size: [
        null,
        [
          CdValidators.composeIf(
            {
              bucket_quota_enabled: true,
              bucket_quota_max_size_unlimited: false
            },
            [Validators.required, this.quotaMaxSizeValidator]
          )
        ]
      ],
      bucket_quota_max_objects_unlimited: [true],
      bucket_quota_max_objects: [
        null,
        [
          Validators.min(0),
          CdValidators.requiredIf({
            bucket_quota_enabled: true,
            bucket_quota_max_objects_unlimited: false
          })
        ]
      ]
    });
  }

  ngOnInit() {
    this.editing = this.router.url.startsWith(`/rgw/user/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    // Process route parameters.
    this.route.params.subscribe((params: { uid: string }) => {
      if (!params.hasOwnProperty('uid')) {
        this.loadingReady();
        return;
      }
      const uid = decodeURIComponent(params.uid);
      // Load the user and quota information.
      const observables = [];
      observables.push(this.rgwUserService.get(uid));
      observables.push(this.rgwUserService.getQuota(uid));
      observableForkJoin(observables).subscribe(
        (resp: any[]) => {
          // Get the default values.
          const defaults = _.clone(this.userForm.value);
          // Extract the values displayed in the form.
          let value = _.pick(resp[0], _.keys(this.userForm.value));
          // Map the max. buckets values.
          switch (value['max_buckets']) {
            case -1:
              value['max_buckets_mode'] = -1;
              value['max_buckets'] = '';
              break;
            case 0:
              value['max_buckets_mode'] = 0;
              value['max_buckets'] = '';
              break;
            default:
              value['max_buckets_mode'] = 1;
              break;
          }
          // Map the quota values.
          ['user', 'bucket'].forEach((type) => {
            const quota = resp[1][type + '_quota'];
            value[type + '_quota_enabled'] = quota.enabled;
            if (quota.max_size < 0) {
              value[type + '_quota_max_size_unlimited'] = true;
              value[type + '_quota_max_size'] = null;
            } else {
              value[type + '_quota_max_size_unlimited'] = false;
              value[type + '_quota_max_size'] = `${quota.max_size} B`;
            }
            if (quota.max_objects < 0) {
              value[type + '_quota_max_objects_unlimited'] = true;
              value[type + '_quota_max_objects'] = null;
            } else {
              value[type + '_quota_max_objects_unlimited'] = false;
              value[type + '_quota_max_objects'] = quota.max_objects;
            }
          });
          // Merge with default values.
          value = _.merge(defaults, value);
          // Update the form.
          this.userForm.setValue(value);

          // Get the sub users.
          this.subusers = resp[0].subusers;

          // Get the keys.
          this.s3Keys = resp[0].keys;
          this.swiftKeys = resp[0].swift_keys;

          // Process the capabilities.
          const mapPerm = { 'read, write': '*' };
          resp[0].caps.forEach((cap: any) => {
            if (cap.perm in mapPerm) {
              cap.perm = mapPerm[cap.perm];
            }
          });
          this.capabilities = resp[0].caps;

          this.loadingReady();
        },
        () => {
          this.loadingError();
        }
      );
    });
  }

  goToListView() {
    this.router.navigate(['/rgw/user']);
  }

  onSubmit() {
    let notificationTitle: string;
    // Exit immediately if the form isn't dirty.
    if (this.userForm.pristine) {
      this.goToListView();
      return;
    }
    const uid = this.userForm.getValue('uid');
    if (this.editing) {
      // Edit
      if (this._isGeneralDirty()) {
        const args = this._getUpdateArgs();
        this.submitObservables.push(this.rgwUserService.update(uid, args));
      }
      notificationTitle = this.i18n(`Updated Object Gateway user '{{uid}}'`, { uid: uid });
    } else {
      // Add
      const args = this._getCreateArgs();
      this.submitObservables.push(this.rgwUserService.create(args));
      notificationTitle = this.i18n(`Created Object Gateway user '{{uid}}'`, { uid: uid });
    }
    // Check if user quota has been modified.
    if (this._isUserQuotaDirty()) {
      const userQuotaArgs = this._getUserQuotaArgs();
      this.submitObservables.push(this.rgwUserService.updateQuota(uid, userQuotaArgs));
    }
    // Check if bucket quota has been modified.
    if (this._isBucketQuotaDirty()) {
      const bucketQuotaArgs = this._getBucketQuotaArgs();
      this.submitObservables.push(this.rgwUserService.updateQuota(uid, bucketQuotaArgs));
    }
    // Finally execute all observables one by one in serial.
    observableConcat(...this.submitObservables).subscribe({
      error: () => {
        // Reset the 'Submit' button.
        this.userForm.setErrors({ cdSubmitButton: true });
      },
      complete: () => {
        this.notificationService.show(NotificationType.success, notificationTitle);
        this.goToListView();
      }
    });
  }

  /**
   * Validate the quota maximum size, e.g. 1096, 1K, 30M or 1.9MiB.
   */
  quotaMaxSizeValidator(control: AbstractControl): ValidationErrors | null {
    if (isEmptyInputValue(control.value)) {
      return null;
    }
    const m = RegExp('^(\\d+(\\.\\d+)?)\\s*(B|K(B|iB)?|M(B|iB)?|G(B|iB)?|T(B|iB)?)?$', 'i').exec(
      control.value
    );
    if (m === null) {
      return { quotaMaxSize: true };
    }
    const bytes = new FormatterService().toBytes(control.value);
    return bytes < 1024 ? { quotaMaxSize: true } : null;
  }

  /**
   * Add/Update a subuser.
   */
  setSubuser(subuser: RgwUserSubuser, index?: number) {
    const mapPermissions: Record<string, string> = {
      'full-control': 'full',
      'read-write': 'readwrite'
    };
    const uid = this.userForm.getValue('uid');
    const args = {
      subuser: subuser.id,
      access:
        subuser.permissions in mapPermissions
          ? mapPermissions[subuser.permissions]
          : subuser.permissions,
      key_type: 'swift',
      secret_key: subuser.secret_key,
      generate_secret: subuser.generate_secret ? 'true' : 'false'
    };
    this.submitObservables.push(this.rgwUserService.createSubuser(uid, args));
    if (_.isNumber(index)) {
      // Modify
      // Create an observable to modify the subuser when the form is submitted.
      this.subusers[index] = subuser;
    } else {
      // Add
      // Create an observable to add the subuser when the form is submitted.
      this.subusers.push(subuser);
      // Add a Swift key. If the secret key is auto-generated, then visualize
      // this to the user by displaying a notification instead of the key.
      this.swiftKeys.push({
        user: subuser.id,
        secret_key: subuser.generate_secret ? 'Apply your changes first...' : subuser.secret_key
      });
    }
    // Mark the form as dirty to be able to submit it.
    this.userForm.markAsDirty();
  }

  /**
   * Delete a subuser.
   * @param {number} index The subuser to delete.
   */
  deleteSubuser(index: number) {
    const subuser = this.subusers[index];
    // Create an observable to delete the subuser when the form is submitted.
    this.submitObservables.push(
      this.rgwUserService.deleteSubuser(this.userForm.getValue('uid'), subuser.id)
    );
    // Remove the associated S3 keys.
    this.s3Keys = this.s3Keys.filter((key) => {
      return key.user !== subuser.id;
    });
    // Remove the associated Swift keys.
    this.swiftKeys = this.swiftKeys.filter((key) => {
      return key.user !== subuser.id;
    });
    // Remove the subuser to update the UI.
    this.subusers.splice(index, 1);
    // Mark the form as dirty to be able to submit it.
    this.userForm.markAsDirty();
  }

  /**
   * Add/Update a capability.
   */
  setCapability(cap: RgwUserCapability, index?: number) {
    const uid = this.userForm.getValue('uid');
    if (_.isNumber(index)) {
      // Modify
      const oldCap = this.capabilities[index];
      // Note, the RadosGW Admin OPS API does not support the modification of
      // user capabilities. Because of that it is necessary to delete it and
      // then to re-add the capability with its new value/permission.
      this.submitObservables.push(
        this.rgwUserService.deleteCapability(uid, oldCap.type, oldCap.perm)
      );
      this.submitObservables.push(this.rgwUserService.addCapability(uid, cap.type, cap.perm));
      this.capabilities[index] = cap;
    } else {
      // Add
      // Create an observable to add the capability when the form is submitted.
      this.submitObservables.push(this.rgwUserService.addCapability(uid, cap.type, cap.perm));
      this.capabilities.push(cap);
    }
    // Mark the form as dirty to be able to submit it.
    this.userForm.markAsDirty();
  }

  /**
   * Delete the given capability:
   * - Delete it from the local array to update the UI
   * - Create an observable that will be executed on form submit
   * @param {number} index The capability to delete.
   */
  deleteCapability(index: number) {
    const cap = this.capabilities[index];
    // Create an observable to delete the capability when the form is submitted.
    this.submitObservables.push(
      this.rgwUserService.deleteCapability(this.userForm.getValue('uid'), cap.type, cap.perm)
    );
    // Remove the capability to update the UI.
    this.capabilities.splice(index, 1);
    // Mark the form as dirty to be able to submit it.
    this.userForm.markAsDirty();
  }

  hasAllCapabilities() {
    return !_.difference(RgwUserCapabilities.getAll(), _.map(this.capabilities, 'type')).length;
  }

  /**
   * Add/Update a S3 key.
   */
  setS3Key(key: RgwUserS3Key, index?: number) {
    if (_.isNumber(index)) {
      // Modify
      // Nothing to do here at the moment.
    } else {
      // Add
      // Split the key's user name into its user and subuser parts.
      const userMatches = key.user.match(/([^:]+)(:(.+))?/);
      // Create an observable to add the S3 key when the form is submitted.
      const uid = userMatches[1];
      const args = {
        subuser: userMatches[2] ? userMatches[3] : '',
        generate_key: key.generate_key ? 'true' : 'false'
      };
      if (args['generate_key'] === 'false') {
        if (!_.isNil(key.access_key)) {
          args['access_key'] = key.access_key;
        }
        if (!_.isNil(key.secret_key)) {
          args['secret_key'] = key.secret_key;
        }
      }
      this.submitObservables.push(this.rgwUserService.addS3Key(uid, args));
      // If the access and the secret key are auto-generated, then visualize
      // this to the user by displaying a notification instead of the key.
      this.s3Keys.push({
        user: key.user,
        access_key: key.generate_key ? 'Apply your changes first...' : key.access_key,
        secret_key: key.generate_key ? 'Apply your changes first...' : key.secret_key
      });
    }
    // Mark the form as dirty to be able to submit it.
    this.userForm.markAsDirty();
  }

  /**
   * Delete a S3 key.
   * @param {number} index The S3 key to delete.
   */
  deleteS3Key(index: number) {
    const key = this.s3Keys[index];
    // Create an observable to delete the S3 key when the form is submitted.
    this.submitObservables.push(
      this.rgwUserService.deleteS3Key(this.userForm.getValue('uid'), key.access_key)
    );
    // Remove the S3 key to update the UI.
    this.s3Keys.splice(index, 1);
    // Mark the form as dirty to be able to submit it.
    this.userForm.markAsDirty();
  }

  /**
   * Show the specified subuser in a modal dialog.
   * @param {number | undefined} index The subuser to show.
   */
  showSubuserModal(index?: number) {
    const uid = this.userForm.getValue('uid');
    const modalRef = this.bsModalService.show(RgwUserSubuserModalComponent);
    if (_.isNumber(index)) {
      // Edit
      const subuser = this.subusers[index];
      modalRef.content.setEditing();
      modalRef.content.setValues(uid, subuser.id, subuser.permissions);
    } else {
      // Add
      modalRef.content.setEditing(false);
      modalRef.content.setValues(uid);
      modalRef.content.setSubusers(this.subusers);
    }
    modalRef.content.submitAction.subscribe((subuser: RgwUserSubuser) => {
      this.setSubuser(subuser, index);
    });
  }

  /**
   * Show the specified S3 key in a modal dialog.
   * @param {number | undefined} index The S3 key to show.
   */
  showS3KeyModal(index?: number) {
    const modalRef = this.bsModalService.show(RgwUserS3KeyModalComponent);
    if (_.isNumber(index)) {
      // View
      const key = this.s3Keys[index];
      modalRef.content.setViewing();
      modalRef.content.setValues(key.user, key.access_key, key.secret_key);
    } else {
      // Add
      const candidates = this._getS3KeyUserCandidates();
      modalRef.content.setViewing(false);
      modalRef.content.setUserCandidates(candidates);
      modalRef.content.submitAction.subscribe((key: RgwUserS3Key) => {
        this.setS3Key(key);
      });
    }
  }

  /**
   * Show the specified Swift key in a modal dialog.
   * @param {number} index The Swift key to show.
   */
  showSwiftKeyModal(index: number) {
    const modalRef = this.bsModalService.show(RgwUserSwiftKeyModalComponent);
    const key = this.swiftKeys[index];
    modalRef.content.setValues(key.user, key.secret_key);
  }

  /**
   * Show the specified capability in a modal dialog.
   * @param {number | undefined} index The S3 key to show.
   */
  showCapabilityModal(index?: number) {
    const modalRef = this.bsModalService.show(RgwUserCapabilityModalComponent);
    if (_.isNumber(index)) {
      // Edit
      const cap = this.capabilities[index];
      modalRef.content.setEditing();
      modalRef.content.setValues(cap.type, cap.perm);
    } else {
      // Add
      modalRef.content.setEditing(false);
      modalRef.content.setCapabilities(this.capabilities);
    }
    modalRef.content.submitAction.subscribe((cap: RgwUserCapability) => {
      this.setCapability(cap, index);
    });
  }

  /**
   * Check if the general user settings (display name, email, ...) have been modified.
   * @return {Boolean} Returns TRUE if the general user settings have been modified.
   */
  private _isGeneralDirty(): boolean {
    return ['display_name', 'email', 'max_buckets_mode', 'max_buckets', 'suspended'].some(
      (path) => {
        return this.userForm.get(path).dirty;
      }
    );
  }

  /**
   * Check if the user quota has been modified.
   * @return {Boolean} Returns TRUE if the user quota has been modified.
   */
  private _isUserQuotaDirty(): boolean {
    return [
      'user_quota_enabled',
      'user_quota_max_size_unlimited',
      'user_quota_max_size',
      'user_quota_max_objects_unlimited',
      'user_quota_max_objects'
    ].some((path) => {
      return this.userForm.get(path).dirty;
    });
  }

  /**
   * Check if the bucket quota has been modified.
   * @return {Boolean} Returns TRUE if the bucket quota has been modified.
   */
  private _isBucketQuotaDirty(): boolean {
    return [
      'bucket_quota_enabled',
      'bucket_quota_max_size_unlimited',
      'bucket_quota_max_size',
      'bucket_quota_max_objects_unlimited',
      'bucket_quota_max_objects'
    ].some((path) => {
      return this.userForm.get(path).dirty;
    });
  }

  /**
   * Helper function to get the arguments of the API request when a new
   * user is created.
   */
  private _getCreateArgs() {
    const result = {
      uid: this.userForm.getValue('uid'),
      display_name: this.userForm.getValue('display_name'),
      suspended: this.userForm.getValue('suspended'),
      email: '',
      max_buckets: this.userForm.getValue('max_buckets'),
      generate_key: this.userForm.getValue('generate_key'),
      access_key: '',
      secret_key: ''
    };
    const email = this.userForm.getValue('email');
    if (_.isString(email) && email.length > 0) {
      _.merge(result, { email: email });
    }
    const generateKey = this.userForm.getValue('generate_key');
    if (!generateKey) {
      _.merge(result, {
        generate_key: false,
        access_key: this.userForm.getValue('access_key'),
        secret_key: this.userForm.getValue('secret_key')
      });
    }
    const maxBucketsMode = parseInt(this.userForm.getValue('max_buckets_mode'), 10);
    if (_.includes([-1, 0], maxBucketsMode)) {
      // -1 => Disable bucket creation.
      //  0 => Unlimited bucket creation.
      _.merge(result, { max_buckets: maxBucketsMode });
    }
    return result;
  }

  /**
   * Helper function to get the arguments for the API request when the user
   * configuration has been modified.
   */
  private _getUpdateArgs() {
    const result: Record<string, any> = {};
    const keys = ['display_name', 'email', 'max_buckets', 'suspended'];
    for (const key of keys) {
      result[key] = this.userForm.getValue(key);
    }
    const maxBucketsMode = parseInt(this.userForm.getValue('max_buckets_mode'), 10);
    if (_.includes([-1, 0], maxBucketsMode)) {
      // -1 => Disable bucket creation.
      //  0 => Unlimited bucket creation.
      result['max_buckets'] = maxBucketsMode;
    }
    return result;
  }

  /**
   * Helper function to get the arguments for the API request when the user
   * quota configuration has been modified.
   */
  private _getUserQuotaArgs(): Record<string, any> {
    const result = {
      quota_type: 'user',
      enabled: this.userForm.getValue('user_quota_enabled'),
      max_size_kb: -1,
      max_objects: -1
    };
    if (!this.userForm.getValue('user_quota_max_size_unlimited')) {
      // Convert the given value to bytes.
      const bytes = new FormatterService().toBytes(this.userForm.getValue('user_quota_max_size'));
      // Finally convert the value to KiB.
      result['max_size_kb'] = (bytes / 1024).toFixed(0) as any;
    }
    if (!this.userForm.getValue('user_quota_max_objects_unlimited')) {
      result['max_objects'] = this.userForm.getValue('user_quota_max_objects');
    }
    return result;
  }

  /**
   * Helper function to get the arguments for the API request when the bucket
   * quota configuration has been modified.
   */
  private _getBucketQuotaArgs(): Record<string, any> {
    const result = {
      quota_type: 'bucket',
      enabled: this.userForm.getValue('bucket_quota_enabled'),
      max_size_kb: -1,
      max_objects: -1
    };
    if (!this.userForm.getValue('bucket_quota_max_size_unlimited')) {
      // Convert the given value to bytes.
      const bytes = new FormatterService().toBytes(this.userForm.getValue('bucket_quota_max_size'));
      // Finally convert the value to KiB.
      result['max_size_kb'] = (bytes / 1024).toFixed(0) as any;
    }
    if (!this.userForm.getValue('bucket_quota_max_objects_unlimited')) {
      result['max_objects'] = this.userForm.getValue('bucket_quota_max_objects');
    }
    return result;
  }

  /**
   * Helper method to get the user candidates for S3 keys.
   * @returns {Array} Returns a list of user identifiers.
   */
  private _getS3KeyUserCandidates() {
    let result = [];
    // Add the current user id.
    const uid = this.userForm.getValue('uid');
    if (_.isString(uid) && !_.isEmpty(uid)) {
      result.push(uid);
    }
    // Append the subusers.
    this.subusers.forEach((subUser) => {
      result.push(subUser.id);
    });
    // Note that it's possible to create multiple S3 key pairs for a user,
    // thus we append already configured users, too.
    this.s3Keys.forEach((key) => {
      result.push(key.user);
    });
    result = _.uniq(result);
    return result;
  }

  onMaxBucketsModeChange(mode: string) {
    if (mode === '1') {
      // If 'Custom' mode is selected, then ensure that the form field
      // 'Max. buckets' contains a valid value. Set it to default if
      // necessary.
      if (!this.userForm.get('max_buckets').valid) {
        this.userForm.patchValue({
          max_buckets: 1000
        });
      }
    }
  }
}
