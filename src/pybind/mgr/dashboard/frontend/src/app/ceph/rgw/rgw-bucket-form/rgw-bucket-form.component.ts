import { Component, OnInit } from '@angular/core';
import {
  AbstractControl,
  AsyncValidatorFn,
  FormBuilder,
  FormGroup,
  ValidationErrors,
  Validators
} from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import * as _ from 'lodash';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { RgwUserService } from '../../../shared/api/rgw-user.service';

@Component({
  selector: 'cd-rgw-bucket-form',
  templateUrl: './rgw-bucket-form.component.html',
  styleUrls: ['./rgw-bucket-form.component.scss']
})
export class RgwBucketFormComponent implements OnInit {
  bucketForm: FormGroup;
  editing = false;
  error = false;
  loading = false;
  owners = null;

  constructor(
    private formBuilder: FormBuilder,
    private route: ActivatedRoute,
    private router: Router,
    private rgwBucketService: RgwBucketService,
    private rgwUserService: RgwUserService
  ) {
    this.createForm();
  }

  createForm() {
    this.bucketForm = this.formBuilder.group({
      id: [null],
      bucket: [null, [Validators.required], [this.bucketNameValidator()]],
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
      (params: { bucket: string }) => {
        if (!params.hasOwnProperty('bucket')) {
          return;
        }
        params.bucket = decodeURIComponent(params.bucket);
        this.loading = true;
        // Load the bucket data in 'edit' mode.
        this.editing = true;
        this.rgwBucketService.get(params.bucket).subscribe((resp: object) => {
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
    const bucketCtl = this.bucketForm.get('bucket');
    const ownerCtl = this.bucketForm.get('owner');
    if (this.editing) {
      // Edit
      const idCtl = this.bucketForm.get('id');
      this.rgwBucketService.update(idCtl.value, bucketCtl.value, ownerCtl.value).subscribe(
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
      this.rgwBucketService.create(bucketCtl.value, ownerCtl.value).subscribe(
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
