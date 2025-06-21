import { Component, OnInit, ViewChild } from '@angular/core';
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
  DEFAULT_PLACEMENT,
  PlacementTarget,
  RequestModel,
  StorageClass,
  Target,
  TierTarget,
  TierType,
  ZoneGroup,
  ZoneGroupDetails
} from '../models/rgw-storage-class.model';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { RgwGlacierStorageclassFormComponent } from '../rgw-glacier-storageclass-form/rgw-glacier-storageclass-form.component';

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
  glacierRestoreDayText: string;
  glacierRestoreTiertypeText: string;
  tiertypeText: string;
  restoreDaysText: string;
  readthroughrestoreDaysText: string;
  restoreStorageClassText: string;
  glacierStorageClassDetails: any;
  @ViewChild(RgwGlacierStorageclassFormComponent, { static: false })
  rgwGlacierStorageClass!: RgwGlacierStorageclassFormComponent;

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
    this.storageClassText = 'A storage class type defines the performance, availability, and cost characteristics of data storage in a system or cloud service.';
    this.multipartMinPartText =
      'It specifies that objects this size or larger are transitioned to the cloud using multipart upload.';
    this.multipartSyncThreholdText =
      'It specifies the minimum part size to use when transitioning objects using multipart upload.';
    this.targetPathText =
      'Target Path refers to the storage location (e.g., bucket or container) in the cloud where data will be stored.';
    this.targetRegionText = 'The region of the remote cloud service where storage is located.';
    this.targetEndpointText = 'The URL endpoint of the remote cloud service for accessing storage.';
    this.targetAccessKeyText =
      "To view or copy your access key, go to your cloud service's user management or credentials section, find your user profile, and locate the access key. You can view and copy the key by following the instructions provided.";

    this.targetSecretKeyText =
      "To view or copy your secret key, go to your cloud service's user management or credentials section, find your user profile, and locate the access key. You can view and copy the key by following the instructions provided.";
    this.retainHeadObjectText =
      'Retain object metadata after transition to the cloud (default: false).';
    this.allowReadThroughText =
      'Enables fetching objects from remote cloud S3 if not found locally (default: false).';
    this.glacierRestoreDayText =
      'Refers to no. of days to the object will be restored on glacier/tape endpoint.';
    this.glacierRestoreTiertypeText = 'Restore retrieval type.';
    this.tiertypeText = 'Restore retrieval type either Standard or Expedited.';
    this.restoreDaysText = 'Refers to no. of days to the object will be restored on glacier/tape endpoint .';
    this.readthroughrestoreDaysText = 'The duration for which objects restored via read-through are retained. Default value is 1 day.';
     this.restoreStorageClassText = 'The storage class to which object data is to be restored. Default value is STANDARD.'
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
          this.storageClassForm.get('storageClassType').setValue(this.tierTargetInfo.val.tier_type); 
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
        });
    }
  }

  // onStorageClassTypeChange(){
  //   console.log(this.rgwGlacierStorageClass?.glacierStorageClassForm, "foo")
  //   if (this.storageClassForm.invalid || this.rgwGlacierStorageClass.glacierStorageClassForm.invalid) {
  //     this.storageClassForm.setErrors({ glacierFormInvalid: true });
  //     this.rgwGlacierStorageClass.glacierStorageClassForm.setErrors({ invalid: true });
  //     Object.keys(this.rgwGlacierStorageClass.glacierStorageClassForm.controls).forEach(key => {
  //       this.rgwGlacierStorageClass.glacierStorageClassForm.get(key).markAsTouched();
  //     });
  //     Object.values(this.rgwGlacierStorageClass.glacierStorageClassForm.controls).forEach(control => {
  //       control.markAsDirty();
  //     });
     
  //   }
  // }

  createForm() {
    this.storageClassForm = this.formBuilder.group({
      storage_class: new FormControl('', {
        validators: [Validators.required]
      }),
      zonegroup: new FormControl(this.selectedZoneGroup, {
        validators: [Validators.required]
      }),
      region: new FormControl('', [
        CdValidators.composeIf({ storageClassType: 'cloud-s3' }, [Validators.required])
      ]),
      placement_target: new FormControl('', {
        validators: [Validators.required]
      }),
      endpoint: new FormControl(null, [
        CdValidators.composeIf({ storageClassType: 'cloud-s3' }, [Validators.required])
      ]),
      access_key: new FormControl(null, [
        CdValidators.composeIf({ storageClassType: 'cloud-s3' }, [Validators.required])
      ]),
      secret_key: new FormControl(null, [
        CdValidators.composeIf({ storageClassType: 'cloud-s3' }, [Validators.required])
      ]),
      target_path: new FormControl('', [
        CdValidators.composeIf({ storageClassType: 'cloud-s3' }, [Validators.required])
      ]),
      retain_head_object: new FormControl(false),
      multipart_sync_threshold: new FormControl(33554432),
      multipart_min_part_size: new FormControl(33554432),
      allow_read_through: new FormControl(false),
      storageClassType: new FormControl('local', Validators.required),
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
    console.log(this.rgwGlacierStorageClass.glacierStorageClassForm, "gla form")
    console.log(this.storageClassForm, "stt")
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

  buildRequest() {
    this.glacierStorageClassDetails = this.rgwGlacierStorageClass?.getGlacierStorageClassFormValue();
    const rawFormValue = _.cloneDeep(this.storageClassForm.value);
    const zoneGroup = this.storageClassForm.get('zonegroup').value;
    const storageClass = this.storageClassForm.get('storage_class').value;
    const placementId = this.storageClassForm.get('placement_target').value;
    const storageClassType = this.storageClassForm.get('storageClassType').value;
 this.storageClassForm.setErrors({ glacierFormInvalid: true });
 Object.values(this.rgwGlacierStorageClass.glacierStorageClassForm.controls).forEach(control => {
  control.markAsDirty();
});
console.log(this.storageClassForm, "erroe")
 if(!this.storageClassForm.errors){
 
    console.log(this.rgwGlacierStorageClass?.glacierStorageClassForm, "gla form")
    console.log(this.storageClassForm, "stt")
    if (storageClassType == TierType.LOCAL) {
      const localRequestModel: RequestModel = {
        zone_group: zoneGroup,
        placement_targets: [
          {
            tags: [],
            placement_id: placementId,
            storage_class: storageClass
          }
        ]
      };
    return localRequestModel;
    } else if (storageClassType == TierType.CLOUD_TIER) {
      const requestModel: RequestModel = {
        zone_group: zoneGroup,
        placement_targets: [
          {
            tags: [],
            placement_id: placementId,
            storage_class: storageClass,
            tier_type: TierType.CLOUD_TIER,
            tier_config: {
              endpoint: rawFormValue.endpoint,
              access_key: rawFormValue.access_key,
              secret: rawFormValue.secret_key,
              target_path: rawFormValue.target_path,
              retain_head_object: rawFormValue.retain_head_object,
              allow_read_through: rawFormValue.allow_read_through,
              region: rawFormValue.region,
              multipart_sync_threshold: rawFormValue.multipart_sync_threshold,
              multipart_min_part_size: rawFormValue.multipart_min_part_size
            }
          }
        ]
      };
      return requestModel;
    } else if (storageClassType == TierType.GLACIER) {
      const requestModel: RequestModel = {
        zone_group: zoneGroup,
        placement_targets: [
          {
            tags: [],
            placement_id: placementId,
            storage_class: storageClass,
            tier_type: TierType.GLACIER,
            tier_config: {
              endpoint: rawFormValue.endpoint,
              access_key: rawFormValue.access_key,
              secret: rawFormValue.secret_key,
              target_path: rawFormValue.target_path,
              retain_head_object: rawFormValue.retain_head_object,
              allow_read_through: rawFormValue.allow_read_through,
              region: rawFormValue.region,
              multipart_sync_threshold: rawFormValue.multipart_sync_threshold,
              multipart_min_part_size: rawFormValue.multipart_min_part_size,
              glacier_restore_days: this.glacierStorageClassDetails.glacier_restore_days,
              glacier_restore_tier_type: this.glacierStorageClassDetails.glacier_restore_tier_type,
              restore_storage_class: this.glacierStorageClassDetails.restore_storage_class,
              readthrough_restore_days: this.glacierStorageClassDetails.readthrough_restore_days
            }
          }
        ]
      };
      return requestModel;
    }
  }
  else {
    this.storageClassForm.invalid;
    }
  return null;
  }
}
