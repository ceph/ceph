

import { Component, Inject, OnInit, Optional } from '@angular/core';
import { FormArray, FormControl, FormGroup, UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { BaseModal } from 'carbon-components-angular';
import { ChangeDetectorRef } from '@angular/core';

@Component({
  selector: 'cd-smb-domain-setting-modal',
  templateUrl: './smb-domain-setting-modal.component.html',
  styleUrls: ['./smb-domain-setting-modal.component.scss']
})
export class SmbDomainSettingModalComponent extends BaseModal implements OnInit {
  domainSettingForm: CdFormGroup;
  realmNames: string[];
  defaultRealmDisabled = false;
  domainSettingResult: any;
  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwRealmService: RgwRealmService,
    public notificationService: NotificationService,
    private cd: ChangeDetectorRef,
    @Optional() @Inject('action') public action: string,
    @Optional() @Inject('resource') public resource: string,
    @Optional() @Inject('domainSettingObject') public domainSettingObject: object[]
  ) {
    super();

    this.action = this.actionLabels.CREATE;
    this.createForm();
  }

  createForm() {
    this.domainSettingForm = new CdFormGroup({
      realmName: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (realmName: string) => {
            return (
              this.action === this.actionLabels.CREATE &&
              this.realmNames &&
              this.realmNames.indexOf(realmName) !== -1
            );
          })
        ]
      }),
      jointSources: new FormArray([])
    });
  }

  ngOnInit(): void {
   
  }

  submit() {
  console.log(this.domainSettingForm);
  this.domainSettingObject = this.domainSettingForm.value;
  console.log(this.domainSettingObject);
  this.closeModal();
  }


  get jointSources() {
    return this.domainSettingForm.get('jointSources') as FormArray;
  }

  addRetentionPolicy() {
    this.jointSources.push(
      new FormGroup({
        source_type1: new FormControl(''),
        ref1: new FormControl('', Validators.required),
      })
    );
    this.cd.detectChanges();
  
  }

  removeRetentionPolicy(index: number) {
    this.jointSources.removeAt(index);
    this.jointSources.controls.forEach((x) =>
      x.get('ref1').updateValueAndValidity()
    );
    this.cd.detectChanges();
  }
}

