import { ChangeDetectorRef, Component, Inject, OnInit, Optional } from '@angular/core';
import { FormArray, FormControl, FormGroup, UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';

import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { SmbService } from '~/app/shared/api/smb.service';
import { CdForm } from '~/app/shared/forms/cd-form';
import { DomainSettings, JoinSource, SMBJoinAuth } from '../smb.model';
import { Observable } from 'rxjs';
import { Router } from '@angular/router';
import { JOINAUTH_URL } from '../smb-join-auth-list/smb-join-auth-list.component';

@Component({
  selector: 'cd-smb-domain-setting-modal',
  templateUrl: './smb-domain-setting-modal.component.html',
  styleUrls: ['./smb-domain-setting-modal.component.scss']
})
export class SmbDomainSettingModalComponent extends CdForm implements OnInit {
  domainSettingsForm: CdFormGroup;
  realmNames: string[];
  joinAuths$: Observable<SMBJoinAuth[]>;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwRealmService: RgwRealmService,
    public notificationService: NotificationService,
    public smbService: SmbService,
    private cd: ChangeDetectorRef,
    private router: Router,
    @Optional() @Inject('action') public action: string,
    @Optional() @Inject('resource') public resource: string,
    @Optional()
    @Inject('domainSettingsObject')
    public domainSettingsObject?: DomainSettings
  ) {
    super();
    this.action = this.actionLabels.UPDATE;
    this.resource = $localize`Active Directory (AD) parameters`;
  }

  private createForm() {
    this.domainSettingsForm = new CdFormGroup({
      realm: new UntypedFormControl('', {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (realm: string) => {
            return this.realmNames && this.realmNames.indexOf(realm) !== -1;
          })
        ]
      }),
      join_sources: new FormArray([])
    });
  }

  ngOnInit(): void {
    this.createForm();
    this.loadingReady();
    this.joinAuths$ = this.smbService.listJoinAuths();
    this.domainSettingsForm.get('realm').setValue(this.domainSettingsObject?.realm);
    const join_sources = this.domainSettingsForm.get('join_sources') as FormArray;

    if (this.domainSettingsObject?.join_sources) {
      this.domainSettingsObject.join_sources.forEach((source: JoinSource) => {
        join_sources.push(this.newJoinSource(source));
      });
    }

    if (!this.domainSettingsObject) {
      this.addJoinSource();
    } else {
      this.action = this.actionLabels.EDIT;
    }
  }

  submit() {
    this.smbService.passData(this.domainSettingsForm.value);
    this.closeModal();
  }

  get join_sources() {
    return this.domainSettingsForm.get('join_sources') as FormArray;
  }

  newJoinSource(joinSource?: JoinSource) {
    return new FormGroup({
      ref: new FormControl(joinSource?.ref || null, Validators.required)
    });
  }

  addJoinSource() {
    this.join_sources.push(this.newJoinSource());
    this.cd.detectChanges();
  }

  navigateCreateJoinSource() {
    this.closeModal();
    this.router.navigate([`${JOINAUTH_URL}/${URLVerbs.CREATE}`]);
  }

  removeJoinSource(index: number) {
    const join_sources = this.domainSettingsForm.get('join_sources') as FormArray;

    if (index >= 0 && index < join_sources.length) {
      join_sources.removeAt(index);
    }

    this.cd.detectChanges();
  }
}
