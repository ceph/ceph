

import { Component, Input, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import _ from 'lodash';
import { ActivatedRoute, Router } from '@angular/router';
import {
  StorageClass,
  TierTarget,
  ZoneGroup,
  ZoneGroupDetails
} from '../models/rgw-storage-class.model';

@Component({
  selector: 'cd-rgw-glacier-storageclass-form',
  templateUrl: './rgw-glacier-storageclass-form.component.html',
  styleUrl: './rgw-glacier-storageclass-form.component.scss'
})
export class RgwGlacierStorageclassFormComponent extends CdForm implements OnInit{
  glacierStorageClassForm: CdFormGroup;
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
  @Input() bucketRateConfig: any;
  @Input() storageClassType: string;
  @Input() validGlacier: boolean = true; 

  constructor(
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
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
    if (this.editing) {
      this.route.params.subscribe((params: StorageClass) => {
        this.storageClassInfo = params;
      });
    }
    if(this.storageClassType === 'cloud-s3-glacier') {
      this.updateValidatorsBasedOnStorageClass();
    }
  }

  createForm() {
    this.glacierStorageClassForm = this.formBuilder.group({
      glacier_restore_tier_type: [
        null,
        Validators.required
      ],   
      glacier_restore_days: new FormControl(
        1,
         
            [Validators.required]
          
      ),
      restore_storage_class: new FormControl(null, Validators.required),
      readthrough_restore_days: new FormControl(1, Validators.required)
    });
  }

  getGlacierStorageClassFormValue() {
    return this._getGlacierStorageClassArgs();
  }


  _getGlacierStorageClassArgs(): any {

    const formValues = {
      glacier_restore_tier_type: this.glacierStorageClassForm.get('glacier_restore_tier_type').value,
      glacier_restore_days: this.glacierStorageClassForm.get('glacier_restore_days').value,
      restore_storage_class: this.glacierStorageClassForm.get('restore_storage_class').value,
      readthrough_restore_days: this.glacierStorageClassForm.get('readthrough_restore_days').value
    };
    return formValues;
  }

  updateValidatorsBasedOnStorageClass() {
    this.glacierStorageClassForm.get('glacier_restore_tier_type').addValidators(Validators.required);
    this.glacierStorageClassForm.get('glacier_restore_days').addValidators(Validators.required);
    this.glacierStorageClassForm.get('restore_storage_class').setValidators(Validators.required);
    this.glacierStorageClassForm.get('readthrough_restore_days').setValidators(Validators.required);
    Object.keys(this.glacierStorageClassForm.controls).forEach(control => {
      this.glacierStorageClassForm.get(control).updateValueAndValidity();
    });
  }
}