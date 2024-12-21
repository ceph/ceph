import { ChangeDetectorRef, Component, Inject, OnInit, Optional } from '@angular/core';
import { FormArray, FormControl, FormGroup, UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { BaseModal } from 'carbon-components-angular';

import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { SmbService } from '~/app/shared/api/smb.service';

@Component({
  selector: 'cd-smb-domain-setting-modal',
  templateUrl: './smb-domain-setting-modal.component.html',
  styleUrls: ['./smb-domain-setting-modal.component.scss']
})
export class SmbDomainSettingModalComponent extends BaseModal implements OnInit {
  domainSettingsForm: CdFormGroup;
  realmNames: string[];

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwRealmService: RgwRealmService,
    public notificationService: NotificationService,
    public smbService: SmbService,
    private cd: ChangeDetectorRef,
    @Optional() @Inject('domainSettingsObject') public domainSettingsObject: any,
    @Optional() @Inject('action') public action: string,
    @Optional() @Inject('resource') public resource: string
  ) {
    super();
    this.action = this.actionLabels.CREATE;
    this.resource = $localize`Domain Setting`;
    this.createForm();
  }

  createForm() {
    this.domainSettingsForm = new CdFormGroup({
      realm: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (realmName: string) => {
            return this.realmNames && this.realmNames.indexOf(realmName) !== -1;
          })
        ]
      }),
      joinSources: new FormArray([])
    });
  }

  ngOnInit(): void {
    this.domainSettingsForm.get('realm').setValue(this.domainSettingsObject?.realm);
    const joinSources = this.domainSettingsForm.get('joinSources') as FormArray;

    if (this.domainSettingsObject?.joinSources) {
      this.domainSettingsObject.joinSources.forEach((source: any) => {
        joinSources.push(
          new FormGroup({
            ref: new FormControl(source.ref || '', Validators.required)
          })
        );
      });
    }

    if (!this.domainSettingsObject) {
      this.joinSources.push(
        new FormGroup({
          ref: new FormControl('', Validators.required)
        })
      );
    } else {
      this.action = this.actionLabels.EDIT;
    }
  }

  submit() {
    this.smbService.passData(this.domainSettingsForm.value);
    this.closeModal();
  }

  get joinSources() {
    return this.domainSettingsForm.get('joinSources') as FormArray;
  }

  addJoinSource() {
    this.joinSources.push(
      new FormGroup({
        ref: new FormControl('', Validators.required)
      })
    );
    this.cd.detectChanges();
  }

  removeJoinSource(index: number) {
    const joinSources = this.domainSettingsForm.get('joinSources') as FormArray;

    if (index >= 0 && index < joinSources.length) {
      joinSources.removeAt(index);
    }

    this.cd.detectChanges();
  }
}
