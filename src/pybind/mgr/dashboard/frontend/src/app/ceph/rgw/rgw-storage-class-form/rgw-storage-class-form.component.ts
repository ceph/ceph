import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
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
  LOCAL_STORAGE_CLASS_TEXT
} from '../models/rgw-storage-class.model';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CdValidators } from '~/app/shared/forms/cd-validators';

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
  targetPathText: string;
  targetEndpointText: string;
  targetRegionText: string;
  showAdvanced: boolean = false;
  defaultZoneGroup: string;
  zonegroupNames: ZoneGroup[];
  placementTargets: string[] = [];
  multipartMinPartText: string;
  storageClassText: string;
  multipartSyncThreholdText: string;
  selectedZoneGroup: string;
  defaultZonegroup: ZoneGroup;
  zoneGroupDetails: ZoneGroupDetails;
  targetSecretKeyText: string;
  targetAccessKeyText: string;
  retainHeadObjectText: string;
  storageClassInfo: StorageClass;
  tierTargetInfo: TierTarget;
  allowReadThroughText: string;
  allowReadThrough: boolean = false;
  TIER_TYPE = TIER_TYPE;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
    private notificationService: NotificationService,
    private rgwStorageService: RgwStorageClassService,
    private rgwZoneGroupService: RgwZonegroupService,
    private router: Router,
    private route: ActivatedRoute
  ) {
    super();
    this.resource = $localize`Tiering Storage Class`;
    this.editing = this.router.url.startsWith(`/rgw/tiering/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
  }

  ngOnInit() {
    this.multipartMinPartText = MULTIPART_MIN_PART_TEXT;
    this.multipartSyncThreholdText = MULTIPART_SYNC_THRESHOLD_TEXT;
    this.targetPathText = TARGET_PATH_TEXT;
    this.targetRegionText = TARGET_REGION_TEXT;
    this.targetEndpointText = TARGET_ENDPOINT_TEXT;
    this.targetAccessKeyText = TARGET_ACCESS_KEY_TEXT;
    this.targetSecretKeyText = TARGET_SECRET_KEY_TEXT;
    this.retainHeadObjectText = RETAIN_HEAD_OBJECT_TEXT;
    this.allowReadThroughText = ALLOW_READ_THROUGH_TEXT;
    this.storageClassText = LOCAL_STORAGE_CLASS_TEXT;
    this.storageClassTypeText();
    this.createForm();
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
          this.storageClassForm.patchValue({
            zonegroup: this.storageClassInfo?.zonegroup_name,
            region: response?.region,
            placement_target: this.storageClassInfo?.placement_target,
            storageClassType: this.tierTargetInfo?.val?.tier_type ?? TIER_TYPE.LOCAL,
            endpoint: response?.endpoint,
            storage_class: this.storageClassInfo?.storage_class,
            access_key: response?.access_key,
            secret_key: response?.secret,
            target_path: response?.target_path,
            retain_head_object: this.tierTargetInfo?.val?.retain_head_object || false,
            multipart_sync_threshold: response?.multipart_sync_threshold || '',
            multipart_min_part_size: response?.multipart_min_part_size || '',
            allow_read_through: this.tierTargetInfo?.val?.allow_read_through || false
          });
        });
    }
    this.storageClassForm?.get('storageClassType')?.valueChanges.subscribe((value) => {
      const controlsToUpdate = ['region', 'endpoint', 'access_key', 'secret_key', 'target_path'];
      controlsToUpdate.forEach((field) => {
        const control = this.storageClassForm.get(field);
        if (
          value === TIER_TYPE.CLOUD_TIER &&
          ['region', 'endpoint', 'access_key', 'secret_key', 'target_path'].includes(field)
        ) {
          control.setValidators([Validators.required]);
        } else {
          control.clearValidators();
        }

        control.updateValueAndValidity();
      });
    });
    this.storageClassForm.get('allow_read_through').valueChanges.subscribe((value) => {
      this.onAllowReadThroughChange(value);
    });
  }

  storageClassTypeText() {
    this.storageClassForm?.get('storageClassType')?.valueChanges.subscribe((value) => {
      if (value === TIER_TYPE.LOCAL) {
        this.storageClassText = LOCAL_STORAGE_CLASS_TEXT;
      } else if (value === TIER_TYPE.CLOUD_TIER) {
        this.storageClassText = CLOUDS3_STORAGE_CLASS_TEXT;
      } else {
        this.storageClassText = LOCAL_STORAGE_CLASS_TEXT;
      }
    });
  }

  createForm() {
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
      endpoint: new FormControl(null, [
        CdValidators.composeIf({ storageClassType: TIER_TYPE.CLOUD_TIER }, [Validators.required])
      ]),
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
      multipart_sync_threshold: new FormControl(33554432),
      multipart_min_part_size: new FormControl(33554432),
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
    const tierTarget = placementTargetInfo.tier_targets.find(
      (target: TierTarget) => target.val.storage_class === storageClass
    );
    return tierTarget;
  }

  onAllowReadThroughChange(checked: boolean): void {
    this.allowReadThrough = checked;
    if (this.allowReadThrough) {
      this.storageClassForm.get('retain_head_object')?.setValue(true);
      this.storageClassForm.get('retain_head_object')?.disable();
    } else {
      this.storageClassForm.get('retain_head_object')?.enable();
    }
  }

  buildRequest() {
    if (this.storageClassForm.errors) return null;

    const rawFormValue = _.cloneDeep(this.storageClassForm.value);
    const zoneGroup = this.storageClassForm.get('zonegroup').value;
    const storageClass = this.storageClassForm.get('storage_class').value;
    const placementId = this.storageClassForm.get('placement_target').value;
    const storageClassType = this.storageClassForm.get('storageClassType').value;
    const retain_head_object = this.storageClassForm.get('retain_head_object').value;

    return this.buildPlacementTargets(
      storageClassType,
      zoneGroup,
      placementId,
      storageClass,
      retain_head_object,
      rawFormValue
    );
  }

  private buildPlacementTargets(
    storageClassType: string,
    zoneGroup: string,
    placementId: string,
    storageClass: string,
    retain_head_object: boolean,
    rawFormValue: any
  ): RequestModel {
    switch (storageClassType) {
      case TIER_TYPE.LOCAL:
        return {
          zone_group: zoneGroup,
          placement_targets: [
            {
              tags: [],
              placement_id: placementId,
              storage_class: storageClass
            }
          ]
        };

      case TIER_TYPE.CLOUD_TIER:
        return {
          zone_group: zoneGroup,
          placement_targets: [
            {
              tags: [],
              placement_id: placementId,
              storage_class: storageClass,
              tier_type: TIER_TYPE.CLOUD_TIER,
              tier_config: {
                endpoint: rawFormValue.endpoint,
                access_key: rawFormValue.access_key,
                secret: rawFormValue.secret_key,
                target_path: rawFormValue.target_path,
                retain_head_object: retain_head_object,
                allow_read_through: rawFormValue.allow_read_through,
                region: rawFormValue.region,
                multipart_sync_threshold: rawFormValue.multipart_sync_threshold,
                multipart_min_part_size: rawFormValue.multipart_min_part_size
              }
            }
          ]
        };
      default:
        return null;
    }
  }
}
