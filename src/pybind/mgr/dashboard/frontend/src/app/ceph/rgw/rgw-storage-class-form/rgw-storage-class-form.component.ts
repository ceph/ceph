import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import _ from 'lodash';
import { Router } from '@angular/router';
import { RgwStorageClassService } from '~/app/shared/api/rgw-storage-class.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import {
  CLOUD_TIER,
  DEFAULT_PLACEMENT,
  RequestModel,
  Target,
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
  zoneGroupDeatils: ZoneGroupDetails;
  targetSecretKeyText: string;
  targetAccessKeyText: string;
  retainHeadObjectText: string;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
    private notificationService: NotificationService,
    private rgwStorageService: RgwStorageClassService,
    private rgwZoneGroupService: RgwZonegroupService,
    private router: Router
  ) {
    super();
    this.resource = $localize`Tiering Storage Class`;
  }

  ngOnInit() {
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
      'Retain object metadata after transition to the cloud (default: deleted).';
    this.action = this.actionLabels.CREATE;
    this.createForm();
    this.loadZoneGroup();
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
      retain_head_object: new FormControl(false),
      multipart_sync_threshold: new FormControl(33554432),
      multipart_min_part_size: new FormControl(33554432)
    });
  }

  loadZoneGroup(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.rgwZoneGroupService.getAllZonegroupsInfo().subscribe(
        (data: ZoneGroupDetails) => {
          this.zoneGroupDeatils = data;
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
    const selectedZoneGroup = this.zoneGroupDeatils.zonegroups.find(
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
    if (defaultPlacementTarget) {
      this.storageClassForm.get('placement_target').setValue(defaultPlacementTarget.name);
    }
  }

  submitAction() {
    const component = this;
    const requestModel = this.buildRequest();
    const storageclassName = this.storageClassForm.get('storage_class').value;
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

  goToListView() {
    this.router.navigate([`rgw/tiering`]);
  }

  buildRequest() {
    const rawFormValue = _.cloneDeep(this.storageClassForm.value);
    const requestModel: RequestModel = {
      zone_group: rawFormValue.zonegroup,
      placement_targets: [
        {
          tags: [],
          placement_id: rawFormValue.placement_target,
          storage_class: rawFormValue.storage_class,
          tier_type: CLOUD_TIER,
          tier_config: {
            endpoint: rawFormValue.endpoint,
            access_key: rawFormValue.access_key,
            secret: rawFormValue.secret_key,
            target_path: rawFormValue.target_path,
            retain_head_object: rawFormValue.retain_head_object,
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
