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
  CLOUD_TIER,
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
  ZoneGroup,
  ZoneGroupDetails
} from '../models/rgw-storage-class.model';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';

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
          let response = this.tierTargetInfo.val.s3;
          this.storageClassForm.get('zonegroup').disable();
          this.storageClassForm.get('placement_target').disable();
          this.storageClassForm.get('storage_class').disable();
          this.storageClassForm.get('zonegroup').setValue(this.storageClassInfo.zonegroup_name);
          this.storageClassForm.get('region').setValue(response.region);
          this.storageClassForm
            .get('placement_target')
            .setValue(this.storageClassInfo.placement_target);
          this.storageClassForm.get('endpoint').setValue(response.endpoint);
          this.storageClassForm.get('storage_class').setValue(this.storageClassInfo.storage_class);
          this.storageClassForm.get('access_key').setValue(response.access_key);
          this.storageClassForm.get('secret_key').setValue(response.secret);
          this.storageClassForm.get('target_path').setValue(response.target_path);
          this.storageClassForm
            .get('retain_head_object')
            .setValue(this.tierTargetInfo?.val?.retain_head_object || false);
          this.storageClassForm
            .get('multipart_sync_threshold')
            .setValue(response.multipart_sync_threshold || '');
          this.storageClassForm
            .get('multipart_min_part_size')
            .setValue(response.multipart_min_part_size || '');
          this.storageClassForm
            .get('allow_read_through')
            .setValue(this.tierTargetInfo?.val?.allow_read_through || false);
        });
    }
    this.storageClassForm.get('allow_read_through').valueChanges.subscribe((value) => {
      this.onAllowReadThroughChange(value);
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
      region: new FormControl('', {
        validators: [Validators.required]
      }),
      placement_target: new FormControl('', {
        validators: [Validators.required]
      }),
      endpoint: new FormControl(null, {
        validators: [Validators.required]
      }),
      access_key: new FormControl(null, Validators.required),
      secret_key: new FormControl(null, Validators.required),
      target_path: new FormControl('', {
        validators: [Validators.required]
      }),
      retain_head_object: new FormControl(true),
      multipart_sync_threshold: new FormControl(33554432),
      multipart_min_part_size: new FormControl(33554432),
      allow_read_through: new FormControl(false)
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
    const rawFormValue = _.cloneDeep(this.storageClassForm.value);
    const zoneGroup = this.storageClassForm.get('zonegroup').value;
    const storageClass = this.storageClassForm.get('storage_class').value;
    const placementId = this.storageClassForm.get('placement_target').value;
    const headObject = this.storageClassForm.get('retain_head_object').value;
    const requestModel: RequestModel = {
      zone_group: zoneGroup,
      placement_targets: [
        {
          tags: [],
          placement_id: placementId,
          storage_class: storageClass,
          tier_type: CLOUD_TIER,
          tier_config: {
            endpoint: rawFormValue.endpoint,
            access_key: rawFormValue.access_key,
            secret: rawFormValue.secret_key,
            target_path: rawFormValue.target_path,
            retain_head_object: headObject,
            allow_read_through: rawFormValue.allow_read_through,
            region: rawFormValue.region,
            multipart_sync_threshold: rawFormValue.multipart_sync_threshold,
            multipart_min_part_size: rawFormValue.multipart_min_part_size
          }
        }
      ]
    };
    return requestModel;
  }
}
