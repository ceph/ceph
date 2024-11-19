import { Component, Inject, OnInit, Optional } from '@angular/core';
import { FormArray, FormControl, FormGroup, UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { BaseModal } from 'carbon-components-angular';
import { ChangeDetectorRef } from '@angular/core';

import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { SmbService } from '~/app/shared/api/smb.service';

@Component({
  selector: 'cd-smb-domain-setting-modal',
  templateUrl: './smb-domain-setting-modal.component.html',
  styleUrls: ['./smb-domain-setting-modal.component.scss']
})
export class SmbDomainSettingModalComponent extends BaseModal implements OnInit {
  domainSettingForm: CdFormGroup;
  realmNames: string[];

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwRealmService: RgwRealmService,
    public notificationService: NotificationService,
    public smbService: SmbService,
    private cd: ChangeDetectorRef,
    @Optional() @Inject('action') public action: string,
    @Optional() @Inject('resource') public resource: string
  ) {
    super();
    this.action = this.actionLabels.CREATE;
    this.createForm();
  }

  createForm() {
    this.domainSettingForm = new CdFormGroup({
      realm: new UntypedFormControl(null, {
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

  ngOnInit(): void {}

  submit() {
    this.smbService.passData(this.domainSettingForm.value);
    this.closeModal();
  }

  get jointSources() {
    return this.domainSettingForm.get('jointSources') as FormArray;
  }

  addJointSource() {
    this.jointSources.push(
      new FormGroup({
        source_type: new FormControl(''),
        ref: new FormControl('', Validators.required)
      })
    );
    this.cd.detectChanges();
  }

  removeJointSource(index: number) {
    this.jointSources.removeAt(index);
    this.jointSources.controls.forEach((x) => x.get('ref').updateValueAndValidity());
    this.cd.detectChanges();
  }
}
