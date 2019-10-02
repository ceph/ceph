import { Component, OnInit } from '@angular/core';
import { AbstractControl, AsyncValidatorFn, ValidationErrors, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { RgwSiteService } from '../../../shared/api/rgw-site.service';
import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { ActionLabelsI18n, URLVerbs } from '../../../shared/constants/app.constants';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { NotificationService } from '../../../shared/services/notification.service';

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
  owners = null;
  action: string;
  resource: string;
  zonegroup: string;
  placementTargets: Object[] = [];

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
    this.bucketForm = this.formBuilder.group({
      id: [null],
      bid: [null, [Validators.required], this.editing ? [] : [this.bucketNameValidator()]],
      owner: [null, [Validators.required]],
      'placement-target': [null, this.editing ? [] : [Validators.required]],
      versioning: [null, this.editing ? [Validators.required] : []]
    });
  }

  ngOnInit() {
    // Get the list of possible owners.
    this.rgwUserService.enumerate().subscribe((resp: string[]) => {
      this.owners = resp.sort();
    });

    if (!this.editing) {
      // Get placement targets:
      this.rgwSiteService.getPlacementTargets().subscribe((placementTargets) => {
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
        // Get the default values.
        const defaults = _.clone(this.bucketForm.value);
        // Extract the values displayed in the form.
        let value = _.pick(resp, _.keys(this.bucketForm.value));
        value['placement-target'] = resp['placement_rule'];
        // Append default values.
        value = _.merge(defaults, value);
        // Update the form.
        this.bucketForm.setValue(value);
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
    const bidCtl = this.bucketForm.get('bid');
    const ownerCtl = this.bucketForm.get('owner');
    const placementTargetCtl = this.bucketForm.get('placement-target');
    if (this.editing) {
      // Edit
      const idCtl = this.bucketForm.get('id');
      const versioningCtl = this.bucketForm.get('versioning');
      this.rgwBucketService
        .update(bidCtl.value, idCtl.value, ownerCtl.value, versioningCtl.value)
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              this.i18n('Updated Object Gateway bucket "{{bid}}".', { bid: bidCtl.value })
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
        .create(bidCtl.value, ownerCtl.value, this.zonegroup, placementTargetCtl.value)
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              this.i18n('Created Object Gateway bucket "{{bid}}"', { bid: bidCtl.value })
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
        constraints.push((name) => {
          const validatorFn = CdValidators.ip();
          return !validatorFn(name);
        });
        // - Bucket names can be between 3 and 63 characters long.
        constraints.push((name) => _.inRange(name.length, 3, 64));
        // - Bucket names must not contain uppercase characters or underscores.
        // - Bucket names must start with a lowercase letter or number.
        // - Bucket names must be a series of one or more labels. Adjacent
        //   labels are separated by a single period (.). Bucket names can
        //   contain lowercase letters, numbers, and hyphens. Each label must
        //   start and end with a lowercase letter or a number.
        constraints.push((name) => {
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
        if (!_.every(constraints, (func) => func(control.value))) {
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
}
