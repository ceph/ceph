import { Component, OnInit } from '@angular/core';
import { AbstractControl, AsyncValidatorFn, ValidationErrors, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import * as _ from 'lodash';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';

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

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private formBuilder: CdFormBuilder,
    private rgwBucketService: RgwBucketService,
    private rgwUserService: RgwUserService
  ) {
    this.createForm();
  }

  createForm() {
    this.bucketForm = this.formBuilder.group({
      id: [null],
      bid: [null, [Validators.required], [this.bucketNameValidator()]],
      owner: [null, [Validators.required]]
    });
  }

  ngOnInit() {
    // Get the list of possible owners.
    this.rgwUserService.enumerate().subscribe((resp: string[]) => {
      this.owners = resp.sort();
    });

    // Process route parameters.
    this.route.params.subscribe(
      (params: { bid: string }) => {
        if (!params.hasOwnProperty('bid')) {
          return;
        }
        const bid = decodeURIComponent(params.bid);
        this.loading = true;
        // Load the bucket data in 'edit' mode.
        this.editing = true;
        this.rgwBucketService.get(bid).subscribe((resp: object) => {
          this.loading = false;
          // Get the default values.
          const defaults = _.clone(this.bucketForm.value);
          // Extract the values displayed in the form.
          let value = _.pick(resp, _.keys(this.bucketForm.value));
          // Append default values.
          value = _.merge(defaults, value);
          // Update the form.
          this.bucketForm.setValue(value);
        });
      },
      (error) => {
        this.error = error;
      }
    );
  }

  goToListView() {
    this.router.navigate(['/rgw/bucket']);
  }

  submit() {
    // Exit immediately if the form isn't dirty.
    if (this.bucketForm.pristine) {
      this.goToListView();
    }
    const bidCtl = this.bucketForm.get('bid');
    const ownerCtl = this.bucketForm.get('owner');
    if (this.editing) {
      // Edit
      const idCtl = this.bucketForm.get('id');
      this.rgwBucketService.update(bidCtl.value, idCtl.value, ownerCtl.value).subscribe(
        () => {
          this.goToListView();
        },
        () => {
          // Reset the 'Submit' button.
          this.bucketForm.setErrors({ cdSubmitButton: true });
        }
      );
    } else {
      // Add
      this.rgwBucketService.create(bidCtl.value, ownerCtl.value).subscribe(
        () => {
          this.goToListView();
        },
        () => {
          // Reset the 'Submit' button.
          this.bucketForm.setErrors({ cdSubmitButton: true });
        }
      );
    }
  }

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
        // Validate the bucket name.
        const nameRe = /^[0-9A-Za-z][\w-\.]{2,254}$/;
        if (!nameRe.test(control.value)) {
          resolve({ bucketNameInvalid: true });
          return;
        }
        // Does any bucket with the given name already exist?
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
