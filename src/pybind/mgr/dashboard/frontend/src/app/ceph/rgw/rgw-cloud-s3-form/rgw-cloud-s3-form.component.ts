import { Component, Input, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import {  Router } from '@angular/router';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
//import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

@Component({
  selector: 'cd-rgw-cloud-s3-form',
  templateUrl: './rgw-cloud-s3-form.component.html',
  styleUrl: './rgw-cloud-s3-form.component.scss'
})
export class RgwCloudS3FormComponent extends CdForm implements OnInit{
  action: string;
  resource: string;
  editing: boolean;
  clouds3StorageClassForm: CdFormGroup;
   @Input() storageClassType: string;

  ngOnInit() {
    this.loadingReady();
    this.createForm();
    if(this.storageClassType === 'cloud-s3') {
      this.updateValidatorsBasedOnStorageClass();
    }
  }

   constructor(
      public actionLabels: ActionLabelsI18n,
      private formBuilder: CdFormBuilder,
      private router: Router,
     // private route: ActivatedRoute
    ) {
      super();
      this.resource = $localize`Tiering Storage Class`;
      this.editing = this.router.url.startsWith(`/rgw/tiering/${URLVerbs.EDIT}`);
      this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    }

      createForm() {
        this.clouds3StorageClassForm = this.formBuilder.group({
          region: new FormControl('', [Validators.required]),
          placement_target: new FormControl('', [Validators.required]),
          endpoint: new FormControl(null,  [Validators.required]),
          access_key: new FormControl(null,  [Validators.required]),
          secret_key: new FormControl(null, 
            [Validators.required]),
          
          target_path: new FormControl('',
             [Validators.required]),
          retain_head_object: new FormControl(false),
          multipart_sync_threshold: new FormControl(33554432),
          multipart_min_part_size: new FormControl(33554432),
          allow_read_through: new FormControl(false),
        });
      }

      getCloudS3StorageClassFormValue() {
        return this._getCloudS3StorageClassArgs();
      }
    
    
      _getCloudS3StorageClassArgs(): any {
    
        const formValues = {
          region: this.clouds3StorageClassForm.get('region').value,
          placement_target: this.clouds3StorageClassForm.get('placement_target').value,
          endpoint: this.clouds3StorageClassForm.get('endpoint').value,
          secret_key: this.clouds3StorageClassForm.get('secret_key').value,
          access_key: this.clouds3StorageClassForm.get('access_key').value,
          target_path: this.clouds3StorageClassForm.get('target_path').value,
          multipart_sync_threshold: this.clouds3StorageClassForm.get('multipart_sync_threshold').value,
          multipart_min_part_size: this.clouds3StorageClassForm.get('multipart_min_part_size').value,
          allow_read_through: this.clouds3StorageClassForm.get('allow_read_through').value,
        };
        return formValues;
      }

      updateValidatorsBasedOnStorageClass() {
        // this.clouds3StorageClassForm.get('region').addValidators(Validators.required);
        // this.clouds3StorageClassForm.get('placement_target').addValidators(Validators.required);
        // this.clouds3StorageClassForm.get('endpoint').addValidators(Validators.required);
        // this.clouds3StorageClassForm.get('secret_key').addValidators(Validators.required);
        // this.clouds3StorageClassForm.get('access_key').addValidators(Validators.required);
        // this.clouds3StorageClassForm.get('target_path').addValidators(Validators.required);
        // this.clouds3StorageClassForm.get('multipart_sync_threshold').addValidators(Validators.required);
        // this.clouds3StorageClassForm.get('multipart_min_part_size').addValidators(Validators.required);
        // this.clouds3StorageClassForm.get('allow_read_through').addValidators(Validators.required);
        // Object.keys(this.clouds3StorageClassForm.controls).forEach(control => {
        //   this.clouds3StorageClassForm.get(control).updateValueAndValidity();
        // });
      }  
}
