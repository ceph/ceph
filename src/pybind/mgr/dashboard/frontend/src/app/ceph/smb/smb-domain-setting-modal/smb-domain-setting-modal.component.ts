

import { Component, Inject, OnInit, Optional } from '@angular/core';
import { FormArray, FormControl, FormGroup, UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { DocService } from '~/app/shared/services/doc.service';
import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-smb-domain-setting-modal',
  templateUrl: './smb-domain-setting-modal.component.html',
  styleUrls: ['./smb-domain-setting-modal.component.scss']
})
export class SmbDomainSettingModalComponent extends BaseModal implements OnInit {
  domainSettingForm: CdFormGroup;
  realmNames: string[];
  newRealmName: string;
  isMaster: boolean;
  defaultRealmDisabled = false;
  docUrl: string;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwRealmService: RgwRealmService,
    public notificationService: NotificationService,
    public docService: DocService,
    @Optional() @Inject('action') public action: string,
    @Optional() @Inject('resource') public resource: string,
    @Optional() @Inject('info') public info: any,
    @Optional() @Inject('multisiteInfo') public multisiteInfo: object[],
    @Optional() @Inject('defaultsInfo') public defaultsInfo: string[],
    @Optional() @Inject('editing') public editing: boolean
  ) {
    super();

    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
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
  }


  get jointSources() {
    return this.domainSettingForm.get('jointSources') as FormArray;
  }

  addRetentionPolicy() {
    this.jointSources.push(
      new FormGroup({
        source_type: new FormControl(''),
        ref: new FormControl('', Validators.required),
      })
    );
   // this.cd.detectChanges();
  
  }
}

