import { Component, OnInit } from '@angular/core';
import { AbstractControl, AsyncValidatorFn, ValidationErrors, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { RgwSiteService } from '../../../shared/api/rgw-site.service';
import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { ActionLabelsI18n, URLVerbs } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { NotificationService } from '../../../shared/services/notification.service';
import { RgwBucketMfaDelete } from '../models/rgw-bucket-mfa-delete';
import { RgwBucketVersioning } from '../models/rgw-bucket-versioning';

@Component({
  selector: 'cd-rgw-bucket-form',
  templateUrl: './rgw-bucket-form.component.html',
  styleUrls: ['./rgw-bucket-form.component.scss']
})
export class RgwBucketFormComponent implements OnInit {
  bucketForm: CdFormGroup;
  editing = false;
  error = false;
  loading = false;
  owners: string[] = null;
  action: string;
  resource: string;
  zonegroup: string;
  placementTargets: object[] = [];
  isVersioningEnabled = false;
  isVersioningAlreadyEnabled = false;
  isMfaDeleteEnabled = false;
  isMfaDeleteAlreadyEnabled = false;
  icons = Icons;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private formBuilder: CdFormBuilder,
    private rgwBucketService: RgwBucketService,
    private rgwSiteService: RgwSiteService,
    private rgwUserService: RgwUserService,
    private notificationService: NotificationService,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    this.editing = this.router.url.startsWith(`/rgw/bucket/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = this.i18n('bucket');
    this.createForm();
  }

  createForm() {
    const self = this;
    const eitherDaysOrYears = CdValidators.custom('eitherDaysOrYears', () => {
      if (!self.bucketForm || !_.get(self.bucketForm.getRawValue(), 'lock_enabled')) {
        return false;
      }
      const years = self.bucketForm.getValue('lock_retention_period_years');
      const days = self.bucketForm.getValue('lock_retention_period_days');
      return (days > 0 && years > 0) || (days === 0 && years === 0);
    });
    const lockPeriodDefinition = [0, [CdValidators.number(false), eitherDaysOrYears]];
    this.bucketForm = this.formBuilder.group({
      id: [null],
      bid: [null, [Validators.required], this.editing ? [] : [this.bucketNameValidator()]],
      owner: [null, [Validators.required]],
      'placement-target': [null, this.editing ? [] : [Validators.required]],
      versioning: [null],
      'mfa-delete': [null],
      'mfa-token-serial': [''],
      'mfa-token-pin': [''],
      lock_enabled: [{ value: false, disabled: this.editing }],
      lock_mode: ['COMPLIANCE'],
      lock_retention_period_days: lockPeriodDefinition,
      lock_retention_period_years: lockPeriodDefinition
    });
  }

  ngOnInit() {
    // Get the list of possible owners.
    this.rgwUserService.enumerate().subscribe((resp: string[]) => {
      this.owners = resp.sort();
    });

    if (!this.editing) {
      // Get placement targets:
      this.rgwSiteService.get('placement-targets').subscribe((placementTargets: any) => {
        this.zonegroup = placementTargets['zonegroup'];
        _.forEach(placementTargets['placement_targets'], (placementTarget) => {
          placementTarget['description'] = `${placementTarget['name']} (${this.i18n('pool')}: ${
            placementTarget['data_pool']
          })`;
          this.placementTargets.push(placementTarget);
        });

        // If there is only 1 placement target, select it by default:
        if (this.placementTargets.length === 1) {
          this.bucketForm.get('placement-target').setValue(this.placementTargets[0]['name']);
        }
      });
    }

    // Process route parameters.
    this.route.params.subscribe((params: { bid: string }) => {
      if (!params.hasOwnProperty('bid')) {
        return;
      }
      const bid = decodeURIComponent(params.bid);
      this.loading = true;

      this.rgwBucketService.get(bid).subscribe((resp: object) => {
        this.loading = false;
        // Get the default values (incl. the values from disabled fields).
        const defaults = _.clone(this.bucketForm.getRawValue());
        // Get the values displayed in the form. We need to do that to
        // extract those key/value pairs from the response data, otherwise
        // the Angular react framework will throw an error if there is no
        // field for a given key.
        let value: object = _.pick(resp, _.keys(defaults));
        value['placement-target'] = resp['placement_rule'];
        // Append default values.
        value = _.merge(defaults, value);
        // Update the form.
        this.bucketForm.setValue(value);
        if (this.editing) {
          this.setVersioningStatus(resp['versioning']);
          this.isVersioningAlreadyEnabled = this.isVersioningEnabled;
          this.setMfaDeleteStatus(resp['mfa_delete']);
          this.isMfaDeleteAlreadyEnabled = this.isMfaDeleteEnabled;
          this.setMfaDeleteValidators();
        }
      });
    });
  }

  goToListView() {
    this.router.navigate(['/rgw/bucket']);
  }

  submit() {
    // Exit immediately if the form isn't dirty.
    if (this.bucketForm.pristine) {
      this.goToListView();
      return;
    }
    const values = this.bucketForm.value;
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
          mfaDelete,
          values['mfa-token-serial'],
          values['mfa-token-pin'],
          values['lock_mode'],
          values['lock_retention_period_days'],
          values['lock_retention_period_years']
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              this.i18n('Updated Object Gateway bucket "{{bid}}".', values)
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
          values['lock_retention_period_years']
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              this.i18n('Created Object Gateway bucket "{{bid}}"', values)
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

  /**
   * Validate the bucket name. In general, bucket names should follow domain
   * name constraints:
   * - Bucket names must be unique.
   * - Bucket names cannot be formatted as IP address.
   * - Bucket names can be between 3 and 63 characters long.
   * - Bucket names must not contain uppercase characters or underscores.
   * - Bucket names must start with a lowercase letter or number.
   * - Bucket names must be a series of one or more labels. Adjacent
   *   labels are separated by a single period (.). Bucket names can
   *   contain lowercase letters, numbers, and hyphens. Each label must
   *   start and end with a lowercase letter or a number.
   */
  bucketNameValidator(): AsyncValidatorFn {
    const rgwBucketService = this.rgwBucketService;
    return (control: AbstractControl): Promise<ValidationErrors | null> => {
      return new Promise((resolve) => {
        // Exit immediately if user has not interacted with the control yet
        // or the control value is empty.
        if (control.pristine || control.value === '') {
          resolve(null);
          return;
        }
        const constraints = [];
        // - Bucket names cannot be formatted as IP address.
        constraints.push((name: AbstractControl) => {
          const validatorFn = CdValidators.ip();
          return !validatorFn(name);
        });
        // - Bucket names can be between 3 and 63 characters long.
        constraints.push((name: string) => _.inRange(name.length, 3, 64));
        // - Bucket names must not contain uppercase characters or underscores.
        // - Bucket names must start with a lowercase letter or number.
        // - Bucket names must be a series of one or more labels. Adjacent
        //   labels are separated by a single period (.). Bucket names can
        //   contain lowercase letters, numbers, and hyphens. Each label must
        //   start and end with a lowercase letter or a number.
        constraints.push((name: string) => {
          const labels = _.split(name, '.');
          return _.every(labels, (label) => {
            // Bucket names must not contain uppercase characters or underscores.
            if (label !== _.toLower(label) || label.includes('_')) {
              return false;
            }
            // Bucket names can contain lowercase letters, numbers, and hyphens.
            if (!/[0-9a-z-]/.test(label)) {
              return false;
            }
            // Each label must start and end with a lowercase letter or a number.
            return _.every([0, label.length], (index) => {
              return /[a-z]/.test(label[index]) || _.isInteger(_.parseInt(label[index]));
            });
          });
        });
        if (!_.every(constraints, (func: Function) => func(control.value))) {
          resolve({ bucketNameInvalid: true });
          return;
        }
        // - Bucket names must be unique.
        rgwBucketService.exists(control.value).subscribe((resp: boolean) => {
          if (!resp) {
            resolve(null);
          } else {
            resolve({ bucketNameExists: true });
          }
        });
      });
    };
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

  setVersioningStatus(status: RgwBucketVersioning) {
    this.isVersioningEnabled = status === RgwBucketVersioning.ENABLED;
  }

  updateVersioning() {
    this.isVersioningEnabled = !this.isVersioningEnabled;
    this.setMfaDeleteValidators();
  }

  getMfaDeleteStatus() {
    return this.isMfaDeleteEnabled ? RgwBucketMfaDelete.ENABLED : RgwBucketMfaDelete.DISABLED;
  }

  setMfaDeleteStatus(status: RgwBucketMfaDelete) {
    this.isMfaDeleteEnabled = status === RgwBucketMfaDelete.ENABLED;
  }

  updateMfaDelete() {
    this.isMfaDeleteEnabled = !this.isMfaDeleteEnabled;
    this.setMfaDeleteValidators();
  }
}
