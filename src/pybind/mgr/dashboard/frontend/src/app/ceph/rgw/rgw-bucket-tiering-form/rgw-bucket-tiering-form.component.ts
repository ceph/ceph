import { ChangeDetectorRef, Component, Inject, OnInit, Optional } from '@angular/core';
import {
  AbstractControl,
  FormArray,
  FormControl,
  FormGroup,
  ValidationErrors,
  Validators
} from '@angular/forms';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Bucket } from '../models/rgw-bucket';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { BucketTieringUtils } from '../utils/rgw-bucket-tiering';
import { StorageClass, ZoneGroupDetails } from '../models/rgw-storage-class.model';
import { CdForm } from '~/app/shared/forms/cd-form';
import { Router } from '@angular/router';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

export interface Tags {
  tagKey: number;
  tagValue: string;
}

@Component({
  selector: 'cd-rgw-bucket-tiering',
  templateUrl: './rgw-bucket-tiering-form.component.html',
  styleUrls: ['./rgw-bucket-tiering-form.component.scss']
})
export class RgwBucketTieringFormComponent extends CdForm implements OnInit {
  tieringForm: CdFormGroup;
  tagsToRemove: Tags[] = [];
  storageClassList: StorageClass[] = null;
  configuredLifecycle: any;
  isStorageClassFetched = false;

  constructor(
    @Inject('bucket') public bucket: Bucket,
    @Optional() @Inject('selectedLifecycle') public selectedLifecycle: any,
    @Optional() @Inject('editing') public editing = false,
    public actionLabels: ActionLabelsI18n,
    private rgwBucketService: RgwBucketService,
    private fb: CdFormBuilder,
    private cd: ChangeDetectorRef,
    private rgwZonegroupService: RgwZonegroupService,
    private notificationService: NotificationService,
    private router: Router
  ) {
    super();
  }

  ngOnInit() {
    this.rgwBucketService
      .getLifecycle(this.bucket.bucket, this.bucket.owner)
      .subscribe((lifecycle) => {
        this.configuredLifecycle = lifecycle || { LifecycleConfiguration: { Rules: [] } };
        if (this.editing) {
          const ruleToEdit = this.configuredLifecycle?.['LifecycleConfiguration']?.['Rules'].filter(
            (rule: any) => rule?.['ID'] === this.selectedLifecycle?.['ID']
          )[0];
          this.tieringForm.patchValue({
            name: ruleToEdit?.['ID'],
            hasPrefix: this.checkIfRuleHasFilters(ruleToEdit),
            prefix:
              ruleToEdit?.['Prefix'] ||
              ruleToEdit?.['Filter']?.['Prefix'] ||
              ruleToEdit?.['Filter']?.['And']?.['Prefix'] ||
              '',
            status: ruleToEdit?.['Status'],
            days: ruleToEdit?.['Transition']?.['Days']
          });
          this.setTags(ruleToEdit);
          this.tieringForm.get('name').disable();
        }
      });
    this.tieringForm = this.fb.group({
      name: [null, [Validators.required, this.duplicateConfigName.bind(this)]],
      storageClass: [null, Validators.required],
      hasPrefix: [false, [Validators.required]],
      prefix: [null, [CdValidators.composeIf({ hasPrefix: true }, [Validators.required])]],
      tags: this.fb.array([]),
      status: ['Enabled', [Validators.required]],
      days: [60, [Validators.required, CdValidators.number(false)]]
    });
    this.loadStorageClass();
  }

  checkIfRuleHasFilters(rule: any) {
    if (
      this.isValidPrefix(rule?.['Prefix']) ||
      this.isValidPrefix(rule?.['Filter']?.['Prefix']) ||
      this.isValidArray(rule?.['Filter']?.['Tags']) ||
      this.isValidPrefix(rule?.['Filter']?.['And']?.['Prefix']) ||
      this.isValidArray(rule?.['Filter']?.['And']?.['Tags'])
    ) {
      return true;
    }
    return false;
  }

  isValidPrefix(value: string) {
    return value !== undefined && value !== '';
  }

  isValidArray(value: object[]) {
    return Array.isArray(value) && value.length > 0;
  }

  setTags(rule: any) {
    if (rule?.['Filter']?.['Tags']?.length > 0) {
      rule?.['Filter']?.['Tags']?.forEach((tag: { Key: string; Value: string }) =>
        this.addTags(tag.Key, tag.Value)
      );
    }
    if (rule?.['Filter']?.['And']?.['Tags']?.length > 0) {
      rule?.['Filter']?.['And']?.['Tags']?.forEach((tag: { Key: string; Value: string }) =>
        this.addTags(tag.Key, tag.Value)
      );
    }
  }

  get tags() {
    return this.tieringForm.get('tags') as FormArray;
  }

  addTags(key?: string, value?: string) {
    this.tags.push(
      new FormGroup({
        Key: new FormControl(key),
        Value: new FormControl(value)
      })
    );
    this.cd.detectChanges();
  }

  duplicateConfigName(control: AbstractControl): ValidationErrors | null {
    if (this.configuredLifecycle?.LifecycleConfiguration?.Rules?.length > 0) {
      const ruleIds = this.configuredLifecycle.LifecycleConfiguration.Rules.map(
        (rule: any) => rule.ID
      );
      return ruleIds.includes(control.value) ? { duplicate: true } : null;
    }
    return null;
  }

  removeTags(idx: number) {
    this.tags.removeAt(idx);
    this.cd.detectChanges();
  }

  loadStorageClass(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.rgwZonegroupService.getAllZonegroupsInfo().subscribe(
        (data: ZoneGroupDetails) => {
          this.storageClassList = [];
          const tierObj = BucketTieringUtils.filterAndMapTierTargets(data);
          this.isStorageClassFetched = true;
          this.storageClassList.push(...tierObj);
          if (this.editing) {
            this.tieringForm
              .get('storageClass')
              .setValue(this.selectedLifecycle?.['Transition']?.['StorageClass']);
          }
          this.loadingReady();
          resolve();
        },
        (error) => {
          reject(error);
        }
      );
    });
  }

  submitTieringConfig() {
    const formValue = this.tieringForm.value;
    if (!this.tieringForm.valid) {
      return;
    }

    let lifecycle: any = {
      ID: this.tieringForm.getRawValue().name,
      Status: formValue.status,
      Transition: [
        {
          Days: formValue.days,
          StorageClass: formValue.storageClass
        }
      ]
    };
    if (formValue.hasPrefix) {
      if (this.tags.length > 0) {
        Object.assign(lifecycle, {
          Filter: {
            And: {
              Prefix: formValue.prefix,
              Tag: this.tags.value
            }
          }
        });
      } else {
        Object.assign(lifecycle, {
          Filter: {
            Prefix: formValue.prefix
          }
        });
      }
    } else {
      Object.assign(lifecycle, {
        Filter: {}
      });
    }
    if (!this.editing) {
      this.configuredLifecycle.LifecycleConfiguration.Rules.push(lifecycle);
      this.rgwBucketService
        .setLifecycle(
          this.bucket.bucket,
          JSON.stringify(this.configuredLifecycle.LifecycleConfiguration),
          this.bucket.owner
        )
        .subscribe({
          next: () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Bucket lifecycle created succesfully`
            );
          },
          error: (error: any) => {
            this.notificationService.show(NotificationType.error, error);
            this.tieringForm.setErrors({ cdSubmitButton: true });
          },
          complete: () => {
            this.closeModal();
          }
        });
    } else {
      const rules = this.configuredLifecycle.LifecycleConfiguration.Rules;
      const index = rules.findIndex((rule: any) => rule?.['ID'] === this.selectedLifecycle?.['ID']);
      rules.splice(index, 1, lifecycle);
      this.rgwBucketService
        .setLifecycle(
          this.bucket.bucket,
          JSON.stringify(this.configuredLifecycle.LifecycleConfiguration),
          this.bucket.owner
        )
        .subscribe({
          next: () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Bucket lifecycle modified succesfully`
            );
          },
          error: (error: any) => {
            this.notificationService.show(NotificationType.error, error);
            this.tieringForm.setErrors({ cdSubmitButton: true });
          },
          complete: () => {
            this.closeModal();
          }
        });
    }
  }

  goToCreateStorageClass() {
    this.router.navigate(['rgw/tiering/create']);
  }
}
