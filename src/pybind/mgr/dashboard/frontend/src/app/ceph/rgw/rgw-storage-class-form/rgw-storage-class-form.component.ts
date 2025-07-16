import { Component, OnInit } from '@angular/core';
import { AbstractControl, FormControl, Validators } from '@angular/forms';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import _ from 'lodash';
import { ActivatedRoute, Router } from '@angular/router';
import { RgwStorageClassService } from '~/app/shared/api/rgw-storage-class.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import {
  ALLOW_READ_THROUGH_TEXT,
  DEFAULT_PLACEMENT,
  MULTIPART_MIN_PART_TEXT,
  MULTIPART_SYNC_THRESHOLD_TEXT,
  PlacementTarget,
  RequestModel,
  RETAIN_HEAD_OBJECT_TEXT,
  StorageClass,
  Target,
  TARGET_ACCESS_KEY_TEXT,
  TARGET_ENDPOINT_TEXT,
  TARGET_PATH_TEXT,
  TARGET_REGION_TEXT,
  TARGET_SECRET_KEY_TEXT,
  TierTarget,
  TIER_TYPE,
  ZoneGroup,
  ZoneGroupDetails,
  CLOUDS3_STORAGE_CLASS_TEXT,
  LOCAL_STORAGE_CLASS_TEXT,
  GLACIER_STORAGE_CLASS_TEXT,
  GLACIER_RESTORE_DAY_TEXT,
  GLACIER_RESTORE_TIER_TYPE_TEXT,
  RESTORE_DAYS_TEXT,
  READTHROUGH_RESTORE_DAYS_TEXT,
  RESTORE_STORAGE_CLASS_TEXT,
  TIER_TYPE_DISPLAY,
  S3Glacier,
  StorageClassOption,
  STORAGE_CLASS_CONSTANTS,
  STANDARD_TIER_TYPE_TEXT,
  EXPEDITED_TIER_TYPE_TEXT,
  TextLabels,
  CLOUD_TIER_REQUIRED_FIELDS,
  GLACIER_REQUIRED_FIELDS
} from '../models/rgw-storage-class.model';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FormatterService } from '~/app/shared/services/formatter.service';

@Component({
  selector: 'cd-rgw-storage-class-form',
  templateUrl: './rgw-storage-class-form.component.html',
  styleUrls: ['./rgw-storage-class-form.component.scss']
})
export class RgwStorageClassFormComponent extends CdForm implements OnInit {
  storageClassForm: CdFormGroup;
  action: string;
  resource: string;
  editing: boolean;
  showAdvanced: boolean = false;
  defaultZoneGroup: string;
  zonegroupNames: ZoneGroup[];
  placementTargets: string[] = [];
  selectedZoneGroup: string;
  defaultZonegroup: ZoneGroup;
  zoneGroupDetails: ZoneGroupDetails;
  storageClassInfo: StorageClass;
  tierTargetInfo: TierTarget;
  glacierStorageClassDetails: S3Glacier;
  allowReadThrough: boolean = false;
  TIER_TYPE = TIER_TYPE;
  TIER_TYPE_DISPLAY = TIER_TYPE_DISPLAY;
  storageClassOptions: StorageClassOption[];
  helpTextLabels: TextLabels;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
    private notificationService: NotificationService,
    private rgwStorageService: RgwStorageClassService,
    private rgwZoneGroupService: RgwZonegroupService,
    private router: Router,
    private route: ActivatedRoute,
    public formatter: FormatterService
  ) {
    super();
    this.resource = $localize`Tiering Storage Class`;
    this.editing = this.router.url.startsWith(`/rgw/tiering/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
  }

  ngOnInit() {
    this.helpTextLabels = {
      targetPathText: TARGET_PATH_TEXT,
      targetEndpointText: TARGET_ENDPOINT_TEXT,
      targetRegionText: TARGET_REGION_TEXT,
      targetAccessKeyText: TARGET_ACCESS_KEY_TEXT,
      targetSecretKeyText: TARGET_SECRET_KEY_TEXT,
      retainHeadObjectText: RETAIN_HEAD_OBJECT_TEXT,
      allowReadThroughText: ALLOW_READ_THROUGH_TEXT,
      storageClassText: LOCAL_STORAGE_CLASS_TEXT,
      multipartMinPartText: MULTIPART_MIN_PART_TEXT,
      multipartSyncThresholdText: MULTIPART_SYNC_THRESHOLD_TEXT,
      tiertypeText: STANDARD_TIER_TYPE_TEXT,
      glacierRestoreDayText: GLACIER_RESTORE_DAY_TEXT,
      glacierRestoreTiertypeText: GLACIER_RESTORE_TIER_TYPE_TEXT,
      restoreDaysText: RESTORE_DAYS_TEXT,
      readthroughrestoreDaysText: READTHROUGH_RESTORE_DAYS_TEXT,
      restoreStorageClassText: RESTORE_STORAGE_CLASS_TEXT
    };
    this.storageClassOptions = [
      { value: TIER_TYPE.LOCAL, label: TIER_TYPE_DISPLAY.LOCAL },
      { value: TIER_TYPE.CLOUD_TIER, label: TIER_TYPE_DISPLAY.CLOUD_TIER },
      { value: TIER_TYPE.GLACIER, label: TIER_TYPE_DISPLAY.GLACIER }
    ];
    this.createForm();
    this.storageClassTypeText();
    this.updateTierTypeHelpText();
    this.loadingReady();
    this.loadZoneGroup();
    if (this.editing) {
      this.route.params.subscribe((params: StorageClass) => {
        this.storageClassInfo = params;
      });
      this.rgwStorageService
        .getPlacement_target(this.storageClassInfo.placement_target)
        .subscribe((placementTargetInfo: PlacementTarget) => {
          this.tierTargetInfo = this.getTierTargetByStorageClass(
            placementTargetInfo,
            this.storageClassInfo.storage_class
          );
          let response = this.tierTargetInfo?.val?.s3;
          this.storageClassForm.get('zonegroup').disable();
          this.storageClassForm.get('placement_target').disable();
          this.storageClassForm.get('storage_class').disable();
          if (
            this.tierTargetInfo?.val?.tier_type === TIER_TYPE.CLOUD_TIER ||
            this.tierTargetInfo?.val?.tier_type === TIER_TYPE.GLACIER
          ) {
            this.storageClassForm.get('storageClassType').disable();
          }
          this.storageClassForm.patchValue({
            zonegroup: this.storageClassInfo?.zonegroup_name,
            region: response?.region,
            placement_target: this.storageClassInfo?.placement_target,
            storageClassType: this.tierTargetInfo?.val?.tier_type ?? TIER_TYPE.LOCAL,
            target_endpoint: response?.endpoint,
            storage_class: this.storageClassInfo?.storage_class,
            access_key: response?.access_key,
            secret_key: response?.secret,
            target_path: response?.target_path,
            retain_head_object: this.tierTargetInfo?.val?.retain_head_object || false,
            multipart_sync_threshold: response?.multipart_sync_threshold || '',
            multipart_min_part_size: response?.multipart_min_part_size || '',
            allow_read_through: this.tierTargetInfo?.val?.allow_read_through || false,
            restore_storage_class: this.tierTargetInfo?.val?.restore_storage_class,
            read_through_restore_days: this.tierTargetInfo?.val?.read_through_restore_days
          });
          if (this.tierTargetInfo?.val?.tier_type == TIER_TYPE.GLACIER) {
            let glacierResponse = this.tierTargetInfo?.val['s3-glacier'];
            this.storageClassForm.patchValue({
              glacier_restore_tier_type: glacierResponse.glacier_restore_tier_type,
              glacier_restore_days: glacierResponse.glacier_restore_days
            });
          }
        });
    }
    this.storageClassForm.get('storageClassType').valueChanges.subscribe((value) => {
      this.updateValidatorsBasedOnStorageClass(value);
    });
    this.storageClassForm.get('allow_read_through').valueChanges.subscribe((value) => {
      this.onAllowReadThroughChange(value);
    });
  }

  private updateValidatorsBasedOnStorageClass(value: string) {
    GLACIER_REQUIRED_FIELDS.forEach((field) => {
      const control = this.storageClassForm.get(field);

      if (
        (value === TIER_TYPE.CLOUD_TIER && CLOUD_TIER_REQUIRED_FIELDS.includes(field)) ||
        (value === TIER_TYPE.GLACIER && GLACIER_REQUIRED_FIELDS.includes(field))
      ) {
        control.setValidators([Validators.required]);
      } else {
        control.clearValidators();
      }
      control.updateValueAndValidity();
    });

    if (this.editing) {
      const defaultValues = {
        allow_read_through: false,
        read_through_restore_days: STORAGE_CLASS_CONSTANTS.DEFAULT_READTHROUGH_RESTORE_DAYS,
        restore_storage_class: STORAGE_CLASS_CONSTANTS.DEFAULT_STORAGE_CLASS,
        multipart_min_part_size: STORAGE_CLASS_CONSTANTS.DEFAULT_MULTIPART_MIN_PART_SIZE,
        multipart_sync_threshold: STORAGE_CLASS_CONSTANTS.DEFAULT_MULTIPART_SYNC_THRESHOLD
      };
      Object.keys(defaultValues).forEach((key) => {
        this.storageClassForm.get(key).setValue(defaultValues[key]);
      });
    }
  }

  storageClassTypeText() {
    this.storageClassForm?.get('storageClassType')?.valueChanges.subscribe((value) => {
      if (value === TIER_TYPE.LOCAL) {
        this.helpTextLabels.storageClassText = LOCAL_STORAGE_CLASS_TEXT;
      } else if (value === TIER_TYPE.CLOUD_TIER) {
        this.helpTextLabels.storageClassText = CLOUDS3_STORAGE_CLASS_TEXT;
      } else if (value === TIER_TYPE.GLACIER) {
        this.helpTextLabels.storageClassText = GLACIER_STORAGE_CLASS_TEXT;
      }
    });
  }

  updateTierTypeHelpText() {
    this.storageClassForm?.get('glacier_restore_tier_type')?.valueChanges.subscribe((value) => {
      if (value === STORAGE_CLASS_CONSTANTS.DEFAULT_STORAGE_CLASS) {
        this.helpTextLabels.tiertypeText = STANDARD_TIER_TYPE_TEXT;
      } else {
        this.helpTextLabels.tiertypeText = EXPEDITED_TIER_TYPE_TEXT;
      }
    });
  }

  createForm() {
    const self = this;

    const lockDaysValidator = CdValidators.custom('lockDays', () => {
      if (!self.storageClassForm || !self.storageClassForm.getRawValue()) {
        return false;
      }

      const lockDays = Number(self.storageClassForm.getValue('read_through_restore_days'));
      return !Number.isInteger(lockDays) || lockDays === 0;
    });
    this.storageClassForm = this.formBuilder.group({
      storage_class: new FormControl('', {
        validators: [Validators.required]
      }),
      zonegroup: new FormControl(this.selectedZoneGroup, {
        validators: [Validators.required]
      }),
      region: new FormControl('', [
        CdValidators.composeIf({ storageClassType: TIER_TYPE.CLOUD_TIER }, [Validators.required])
      ]),
      placement_target: new FormControl('', {
        validators: [Validators.required]
      }),
      target_endpoint: new FormControl(null, {
        validators: [CdValidators.url, Validators.required]
      }),
      access_key: new FormControl(null, [
        CdValidators.composeIf({ storageClassType: TIER_TYPE.CLOUD_TIER }, [Validators.required])
      ]),
      secret_key: new FormControl(null, [
        CdValidators.composeIf({ storageClassType: TIER_TYPE.CLOUD_TIER }, [Validators.required])
      ]),
      target_path: new FormControl('', [
        CdValidators.composeIf({ storageClassType: TIER_TYPE.CLOUD_TIER }, [Validators.required])
      ]),
      retain_head_object: new FormControl(true),
      glacier_restore_tier_type: new FormControl(STORAGE_CLASS_CONSTANTS.DEFAULT_STORAGE_CLASS, [
        CdValidators.composeIf({ storageClassType: TIER_TYPE.GLACIER }, [Validators.required])
      ]),
      glacier_restore_days: new FormControl(STORAGE_CLASS_CONSTANTS.DEFAULT_GLACIER_RESTORE_DAYS, [
        CdValidators.composeIf({ storageClassType: TIER_TYPE.GLACIER || TIER_TYPE.CLOUD_TIER }, [
          CdValidators.number(false),
          lockDaysValidator
        ])
      ]),
      restore_storage_class: new FormControl(STORAGE_CLASS_CONSTANTS.DEFAULT_STORAGE_CLASS),
      read_through_restore_days: new FormControl(
        {
          value: STORAGE_CLASS_CONSTANTS.DEFAULT_READTHROUGH_RESTORE_DAYS,
          disabled: true
        },
        CdValidators.composeIf(
          (form: AbstractControl) => {
            const type = form.get('storageClassType')?.value;
            return type === TIER_TYPE.GLACIER || type === TIER_TYPE.CLOUD_TIER;
          },
          [CdValidators.number(false), lockDaysValidator]
        )
      ),
      multipart_sync_threshold: new FormControl(
        STORAGE_CLASS_CONSTANTS.DEFAULT_MULTIPART_SYNC_THRESHOLD
      ),
      multipart_min_part_size: new FormControl(
        STORAGE_CLASS_CONSTANTS.DEFAULT_MULTIPART_MIN_PART_SIZE
      ),
      allow_read_through: new FormControl(false),
      storageClassType: new FormControl(TIER_TYPE.LOCAL, Validators.required)
    });
  }

  loadZoneGroup(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.rgwZoneGroupService.getAllZonegroupsInfo().subscribe(
        (data: ZoneGroupDetails) => {
          this.zoneGroupDetails = data;
          this.zonegroupNames = [];
          this.placementTargets = [];
          if (data.zonegroups && data.zonegroups.length > 0) {
            this.zonegroupNames = data.zonegroups.map((zoneGroup: ZoneGroup) => {
              return {
                id: zoneGroup.id,
                name: zoneGroup.name
              };
            });
          }
          this.defaultZonegroup = this.zonegroupNames.find(
            (zonegroups: ZoneGroup) => zonegroups.id === data.default_zonegroup
          );
          this.storageClassForm.get('zonegroup').setValue(this.defaultZonegroup.name);
          this.onZonegroupChange();
          resolve();
        },
        (error) => reject(error)
      );
    });
  }

  onZonegroupChange() {
    const zoneGroupControl = this.storageClassForm.get('zonegroup').value;
    const selectedZoneGroup = this.zoneGroupDetails.zonegroups.find(
      (zonegroup) => zonegroup.name === zoneGroupControl
    );
    const defaultPlacementTarget = selectedZoneGroup.placement_targets.find(
      (target: Target) => target.name === DEFAULT_PLACEMENT
    );
    if (selectedZoneGroup) {
      const placementTargetNames = selectedZoneGroup.placement_targets.map(
        (target: Target) => target.name
      );
      this.placementTargets = placementTargetNames;
    }
    if (defaultPlacementTarget && !this.editing) {
      this.storageClassForm.get('placement_target').setValue(defaultPlacementTarget.name);
    } else {
      this.storageClassForm
        .get('placement_target')
        .setValue(this.storageClassInfo.placement_target);
    }
  }

  submitAction() {
    const component = this;
    const requestModel = this.buildRequest();
    const storageclassName = this.storageClassForm.get('storage_class').value;
    if (this.editing) {
      this.rgwStorageService.editStorageClass(requestModel).subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Updated Storage Class '${storageclassName}'`
          );
          this.goToListView();
        },
        () => {
          component.storageClassForm.setErrors({ cdSubmitButton: true });
        }
      );
    } else {
      this.rgwStorageService.createStorageClass(requestModel).subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Created Storage Class '${storageclassName}'`
          );
          this.goToListView();
        },
        () => {
          component.storageClassForm.setErrors({ cdSubmitButton: true });
        }
      );
    }
  }

  goToListView() {
    this.router.navigate([`rgw/tiering`]);
  }

  getTierTargetByStorageClass(placementTargetInfo: PlacementTarget, storageClass: string) {
    const tierTarget = placementTargetInfo?.tier_targets?.find(
      (target: TierTarget) => target.val.storage_class === storageClass
    );
    return tierTarget;
  }

  onAllowReadThroughChange(checked: boolean): void {
    this.allowReadThrough = checked;
    const readThroughDaysControl = this.storageClassForm.get('read_through_restore_days');
    if (this.allowReadThrough) {
      this.storageClassForm.get('retain_head_object')?.setValue(true);
      this.storageClassForm.get('retain_head_object')?.disable();
      readThroughDaysControl?.enable();
    } else {
      this.storageClassForm.get('retain_head_object')?.enable();
      readThroughDaysControl?.disable();
    }
  }

  isTierMatch(...types: string[]): boolean {
    const tierType = this.storageClassForm.getValue('storageClassType');
    return types.includes(tierType);
  }

  buildRequest() {
    if (this.storageClassForm.errors) return null;

    const rawFormValue = _.cloneDeep(this.storageClassForm.value);
    const zoneGroup = this.storageClassForm.get('zonegroup').value;
    const storageClass = this.storageClassForm.get('storage_class').value;
    const placementId = this.storageClassForm.get('placement_target').value;
    const storageClassType = this.storageClassForm.get('storageClassType').value;
    const retain_head_object = this.storageClassForm.get('retain_head_object').value;
    const multipart_min_part_size = this.formatter.toBytes(
      this.storageClassForm.get('multipart_min_part_size').value
    );
    const multipart_sync_threshold = this.formatter.toBytes(
      this.storageClassForm.get('multipart_sync_threshold').value
    );
    return this.buildPlacementTargets(
      storageClassType,
      zoneGroup,
      placementId,
      storageClass,
      retain_head_object,
      rawFormValue,
      multipart_sync_threshold,
      multipart_min_part_size
    );
  }

  private buildPlacementTargets(
    storageClassType: string,
    zoneGroup: string,
    placementId: string,
    storageClass: string,
    retain_head_object: boolean,
    rawFormValue: any,
    multipart_sync_threshold: number,
    multipart_min_part_size: number
  ): RequestModel {
    const baseTarget = {
      placement_id: placementId,
      storage_class: storageClass
    };

    if (storageClassType === TIER_TYPE.LOCAL) {
      return {
        zone_group: zoneGroup,
        placement_targets: [baseTarget]
      };
    }

    const tierConfig = {
      endpoint: rawFormValue.target_endpoint,
      access_key: rawFormValue.access_key,
      secret: rawFormValue.secret_key,
      target_path: rawFormValue.target_path,
      retain_head_object,
      allow_read_through: rawFormValue.allow_read_through,
      region: rawFormValue.region,
      multipart_sync_threshold: multipart_sync_threshold,
      multipart_min_part_size: multipart_min_part_size,
      restore_storage_class: rawFormValue.restore_storage_class,
      ...(rawFormValue.allow_read_through
        ? { read_through_restore_days: rawFormValue.read_through_restore_days }
        : {})
    };

    if (storageClassType === TIER_TYPE.CLOUD_TIER) {
      return {
        zone_group: zoneGroup,
        placement_targets: [
          {
            ...baseTarget,
            tier_type: TIER_TYPE.CLOUD_TIER,
            tier_config: {
              ...tierConfig
            }
          }
        ]
      };
    }

    if (storageClassType === TIER_TYPE.GLACIER) {
      return {
        zone_group: zoneGroup,
        placement_targets: [
          {
            ...baseTarget,
            tier_type: TIER_TYPE.GLACIER,
            tier_config: {
              ...tierConfig,
              glacier_restore_days: rawFormValue.glacier_restore_days,
              glacier_restore_tier_type: rawFormValue.glacier_restore_tier_type
            }
          }
        ]
      };
    }
    return {
      zone_group: zoneGroup,
      placement_targets: [baseTarget]
    };
  }
}
