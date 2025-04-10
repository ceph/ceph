import { Component, OnInit } from '@angular/core';
import { AbstractControl, AsyncValidatorFn, ValidationErrors, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { Observable, timer as observableTimer, of } from 'rxjs';
import {
  catchError,
  debounceTime,
  distinctUntilChanged,
  map,
  mergeMap,
  switchMapTo
} from 'rxjs/operators';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwMultisiteSyncPolicyStatus } from '../models/rgw-multisite';
import { CdForm } from '~/app/shared/forms/cd-form';
import _ from 'lodash';

@Component({
  selector: 'cd-rgw-multisite-sync-policy-form',
  templateUrl: './rgw-multisite-sync-policy-form.component.html',
  styleUrls: ['./rgw-multisite-sync-policy-form.component.scss']
})
export class RgwMultisiteSyncPolicyFormComponent extends CdForm implements OnInit {
  syncPolicyForm: CdFormGroup;
  editing = false;
  action: string;
  resource: string;
  syncPolicyStatus = RgwMultisiteSyncPolicyStatus;
  pageURL: string;
  bucketDataSource = (text$: Observable<string>) => {
    return text$.pipe(
      debounceTime(200),
      distinctUntilChanged(),
      mergeMap((token: string) => this.getBucketTypeahead(token))
    );
  };

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    public actionLabels: ActionLabelsI18n,
    private fb: CdFormBuilder,
    private rgwMultisiteService: RgwMultisiteService,
    private notificationService: NotificationService,
    private rgwBucketService: RgwBucketService
  ) {
    super();
    this.editing = this.router.url.includes('(modal:edit');
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = $localize`Sync Policy Group`;
    this.createForm();
    this.loadingReady();
    this.pageURL = 'rgw/multisite/sync-policy';
  }

  ngOnInit(): void {
    if (this.editing) {
      this.route.paramMap.subscribe((params: any) => {
        const groupName = params.get('groupName');
        if (groupName) {
          const bucketName = params.get('bucketName');
          this.loadingStart();
          this.rgwMultisiteService
            .getSyncPolicyGroup(groupName, bucketName)
            .subscribe((syncPolicy: any) => {
              this.loadingReady();
              if (syncPolicy) {
                this.syncPolicyForm.get('bucket_name').disable();
                this.syncPolicyForm.patchValue({
                  group_id: syncPolicy.id,
                  status: syncPolicy.status,
                  bucket_name: bucketName
                });
              } else {
                this.goToListView();
              }
            });
        }
      });
    }
  }

  goToListView() {
    // passing state in order to return to same tab on details page
    this.router.navigate([this.pageURL, { outlets: { modal: null }, state: { reload: true } }]);
  }

  createForm() {
    this.syncPolicyForm = this.fb.group({
      group_id: ['', Validators.required],
      status: [`${this.syncPolicyStatus.ENABLED}`, Validators.required],
      bucket_name: ['', , this.bucketExistence(true)]
    });
  }

  submit() {
    if (this.syncPolicyForm.pristine) {
      this.goToListView();
      return;
    }

    // Ensure that no validation is pending
    if (this.syncPolicyForm.pending) {
      this.syncPolicyForm.setErrors({ cdSubmitButton: true });
      return;
    }

    if (!this.editing) {
      // Add
      this.rgwMultisiteService.createSyncPolicyGroup(this.syncPolicyForm.getRawValue()).subscribe(
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
    } else {
      this.rgwMultisiteService.modifySyncPolicyGroup(this.syncPolicyForm.getRawValue()).subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Modified Sync Policy Group '${this.syncPolicyForm.getValue('group_id')}'`
          );
          this.goToListView();
        },
        () => {
          // Reset the 'Submit' button.
          this.syncPolicyForm.setErrors({ cdSubmitButton: true });
        }
      );
    }
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

  private getBucketTypeahead(path: string): Observable<any> {
    if (_.isString(path) && path !== '/' && path !== '') {
      return this.rgwBucketService.list().pipe(
        map((bucketList: any) =>
          bucketList
            .filter((bucketName: string) => bucketName.toLowerCase().includes(path))
            .slice(0, 15)
        ),
        catchError(() => of([$localize`Error while retrieving bucket names.`]))
      );
    } else {
      return of([]);
    }
  }
}
