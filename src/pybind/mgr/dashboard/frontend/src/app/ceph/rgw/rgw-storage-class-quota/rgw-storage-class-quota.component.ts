import { Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { AbstractControl, FormArray, ValidationErrors, Validators } from '@angular/forms';

import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { StorageClassQuota } from '../models/rgw-user';
import { ZoneGroupDetails } from '../models/rgw-storage-class.model';
import { BucketTieringUtils } from '../utils/rgw-bucket-tiering';

@Component({
  selector: 'cd-rgw-storage-class-quota',
  templateUrl: './rgw-storage-class-quota.component.html',
  styleUrls: ['./rgw-storage-class-quota.component.scss'],
  standalone: false
})
export class RgwStorageClassQuotaComponent implements OnInit, OnChanges {
  @Input()
  idPrefix = 'sc-quota';

  @Input()
  title = $localize`Storage class quotas`;

  @Input()
  description = $localize`Set object-count and size limits per storage class so hot and cold tiers can be capped separately.`;

  @Input()
  savedQuotas?: StorageClassQuota[] | Record<string, StorageClassQuota>;

  form: CdFormGroup;

  constructor(
    private formBuilder: CdFormBuilder,
    private rgwZonegroupService: RgwZonegroupService
  ) {
    this.form = this.formBuilder.group({
      quotas: this.formBuilder.array([])
    });
    this.syncQuotaRows(['STANDARD']);
  }

  ngOnInit(): void {
    this.rgwZonegroupService.getAllZonegroupsInfo().subscribe({
      next: (data: ZoneGroupDetails) => {
        this.syncQuotaRows(BucketTieringUtils.getStorageClassNames(data));
      },
      error: () => {
        this.syncQuotaRows(['STANDARD']);
      }
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.savedQuotas && this.quotas.length > 0) {
      this.applySavedQuotas();
    }
  }

  get quotas(): FormArray {
    return this.form.get('quotas') as FormArray;
  }

  getStorageClassQuotas(): StorageClassQuota[] {
    return this.quotas.controls.map((control) => {
      const group = control as CdFormGroup;
      const unlimitedSize = group.getValue('max_size_unlimited');
      const unlimitedObjects = group.getValue('max_objects_unlimited');
      let maxSize = -1;
      if (group.getValue('enabled') && !unlimitedSize) {
        maxSize = new FormatterService().toBytes(group.getValue('max_size')) || -1;
      }
      return {
        storage_class: group.getValue('storage_class'),
        enabled: group.getValue('enabled'),
        max_size: maxSize,
        max_objects:
          group.getValue('enabled') && !unlimitedObjects
            ? Number(group.getValue('max_objects'))
            : -1
      };
    });
  }

  isDirty(): boolean {
    return this.form.dirty;
  }

  private syncQuotaRows(storageClasses: string[]): void {
    const existing = new Map(
      this.quotas.controls.map((control) => {
        const group = control as CdFormGroup;
        return [group.getValue('storage_class') as string, group];
      })
    );
    this.quotas.clear();
    storageClasses.forEach((storageClass) => {
      this.quotas.push(existing.get(storageClass) || this.createQuotaGroup(storageClass));
    });
    this.applySavedQuotas();
  }

  private createQuotaGroup(storageClass: string, quota?: StorageClassQuota): CdFormGroup {
    const enabled = quota?.enabled ?? false;
    const unlimitedSize = !quota || quota.max_size < 0;
    const unlimitedObjects = !quota || quota.max_objects < 0;
    return this.formBuilder.group({
      storage_class: [storageClass],
      enabled: [enabled],
      max_size_unlimited: [unlimitedSize],
      max_size: [
        unlimitedSize || !quota ? null : `${quota.max_size} B`,
        [
          CdValidators.composeIf(
            {
              enabled: true,
              max_size_unlimited: false
            },
            [Validators.required, this.quotaMaxSizeValidator]
          )
        ]
      ],
      max_objects_unlimited: [unlimitedObjects],
      max_objects: [
        unlimitedObjects || !quota ? null : quota.max_objects,
        [
          CdValidators.requiredIf({
            enabled: true,
            max_objects_unlimited: false
          })
        ]
      ]
    });
  }

  private applySavedQuotas(): void {
    const saved = this.normalizeSavedQuotas(this.savedQuotas);
    if (!saved.length || this.quotas.length === 0) {
      return;
    }
    saved.forEach((quota) => {
      const index = this.quotas.controls.findIndex(
        (control) => (control as CdFormGroup).getValue('storage_class') === quota.storage_class
      );
      if (index >= 0) {
        this.quotas.setControl(index, this.createQuotaGroup(quota.storage_class, quota));
      } else {
        this.quotas.push(this.createQuotaGroup(quota.storage_class, quota));
      }
    });
  }

  private normalizeSavedQuotas(
    quotas?: StorageClassQuota[] | Record<string, StorageClassQuota>
  ): StorageClassQuota[] {
    if (!quotas) {
      return [];
    }
    if (Array.isArray(quotas)) {
      return quotas;
    }
    return Object.entries(quotas).map(([storageClass, quota]) => ({
      ...quota,
      storage_class: quota.storage_class || storageClass
    }));
  }

  quotaMaxSizeValidator = (control: AbstractControl): ValidationErrors | null => {
    return new FormatterService().performValidation(
      control,
      '^(\\d+(\\.\\d+)?)\\s*(B|K(B|iB)?|M(B|iB)?|G(B|iB)?|T(B|iB)?)?$',
      { quotaMaxSize: true },
      'quota'
    );
  };
}
