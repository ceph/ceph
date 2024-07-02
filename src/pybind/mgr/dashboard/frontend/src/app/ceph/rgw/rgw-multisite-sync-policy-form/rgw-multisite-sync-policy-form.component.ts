import { Component } from '@angular/core';
import { AbstractControl, AsyncValidatorFn, ValidationErrors, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { Observable, timer as observableTimer, of } from 'rxjs';
import { map, switchMapTo } from 'rxjs/operators';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwMultisiteSyncPolicyStatus } from '../models/rgw-multisite';

@Component({
  selector: 'cd-rgw-multisite-sync-policy-form',
  templateUrl: './rgw-multisite-sync-policy-form.component.html',
  styleUrls: ['./rgw-multisite-sync-policy-form.component.scss']
})
export class RgwMultisiteSyncPolicyFormComponent {
  syncPolicyForm: CdFormGroup;
  editing = false;
  action: string;
  resource: string;
  syncPolicyStatus = RgwMultisiteSyncPolicyStatus;

  constructor(
    private router: Router,
    public actionLabels: ActionLabelsI18n,
    private fb: CdFormBuilder,
    private rgwMultisiteService: RgwMultisiteService,
    private notificationService: NotificationService,
    private rgwBucketService: RgwBucketService
  ) {
    this.editing = this.router.url.startsWith(`/rgw/multisite/sync-policy/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = $localize`Sync Policy Group`;
    this.createForm();
  }

  createForm() {
    this.syncPolicyForm = this.fb.group({
      group_id: ['', Validators.required],
      status: [`${this.syncPolicyStatus.ENABLED}`, Validators.required],
      bucket_name: ['', , this.bucketExistence(true)]
    });
  }

  goToListView() {
    // passing state in order to return to same tab on details page
    this.router.navigate(['/rgw/multisite'], { state: { activeId: 'syncPolicy' } });
  }

  submit() {
    if (this.syncPolicyForm.pristine || this.syncPolicyForm.invalid) {
      return;
    }

    // Ensure that no validation is pending
    if (this.syncPolicyForm.pending) {
      this.syncPolicyForm.setErrors({ cdSubmitButton: true });
      return;
    }

    // Add
    this.rgwMultisiteService.createSyncPolicyGroup(this.syncPolicyForm.value).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Created Sync Policy Group '${this.syncPolicyForm.getValue('group_id')}'`
        );
        this.goToListView();
      },
      () => {
        // Reset the 'Submit' button.
        this.syncPolicyForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  bucketExistence(requiredExistenceResult: boolean): AsyncValidatorFn {
    return (control: AbstractControl): Observable<ValidationErrors | null> => {
      if (control.dirty) {
        return observableTimer(500).pipe(
          switchMapTo(this.rgwBucketService.exists(control.value)),
          map((existenceResult: boolean) =>
            existenceResult === requiredExistenceResult ? null : { bucketNameNotAllowed: true }
          )
        );
      }
      return of(null);
    };
  }
}
