import { Component, Inject, OnInit, Optional } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm } from '../models/rgw-multisite';
import { DocService } from '~/app/shared/services/doc.service';
import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-rgw-multisite-realm-form',
  templateUrl: './rgw-multisite-realm-form.component.html',
  styleUrls: ['./rgw-multisite-realm-form.component.scss']
})
export class RgwMultisiteRealmFormComponent extends BaseModal implements OnInit {
  realmAction: string;
  multisiteRealmForm: CdFormGroup;
  realmDetail: any;
  editing = false;
  resource: string;
  multisiteRealmDetail: object[] = [];
  realm: RgwRealm;
  realmList: RgwRealm[] = [];
  zonegroupList: RgwRealm[] = [];
  realmNames: string[];
  newRealmName: string;
  isMaster: boolean;
  defaultsInfo: string[];
  defaultRealmDisabled = false;
  docUrl: string;
  realmData: any;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwRealmService: RgwRealmService,
    public notificationService: NotificationService,
    public docService: DocService,
    @Optional() @Inject('data') public data?: object
  ) {
    super();
    this.data = this.data || {};
    this.realmData = this.data;

    this.realmAction = this.editing
      ? this.actionLabels.EDIT + this.realmData.resource
      : this.actionLabels.CREATE + this.realmData.resource;
    this.createForm();
  }

  createForm() {
    this.multisiteRealmForm = new CdFormGroup({
      realmName: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (realmName: string) => {
            return (
              this.realmData.action === 'create' &&
              this.realmNames &&
              this.realmNames.indexOf(realmName) !== -1
            );
          })
        ]
      }),
      default_realm: new UntypedFormControl(false)
    });
  }

  ngOnInit(): void {
    this.realmList =
      this.realmData.multisiteInfo[0] !== undefined &&
      this.realmData.multisiteInfo[0].hasOwnProperty('realms')
        ? this.realmData.multisiteInfo[0]['realms']
        : [];
    this.realmNames = this.realmList.map((realm) => {
      return realm['name'];
    });
    if (this.realmData.action === 'edit') {
      this.zonegroupList =
        this.realmData.multisiteInfo[1] !== undefined &&
        this.realmData.multisiteInfo[1].hasOwnProperty('zonegroups')
          ? this.realmData.multisiteInfo[1]['zonegroups']
          : [];
      this.multisiteRealmForm.get('realmName').setValue(this.realmData.info.data.name);
      this.multisiteRealmForm
        .get('default_realm')
        .setValue(this.realmData.info.data.is_default);
      if (this.realmData.info.data.is_default) {
        this.multisiteRealmForm.get('default_realm').disable();
      }
    }
    this.zonegroupList.forEach((zgp: any) => {
      if (zgp.is_master === true && zgp.realm_id === this.realmData.info.data.id) {
        this.isMaster = true;
      }
    });
    if (
      this.realmData.defaultsInfo &&
      this.realmData.defaultsInfo['defaultRealmName'] !== null
    ) {
      this.multisiteRealmForm.get('default_realm').disable();
      this.defaultRealmDisabled = true;
    }
    this.docUrl = this.docService.urlGenerator('rgw-multisite');
  }

  submit() {
    const values = this.multisiteRealmForm.getRawValue();
    this.realm = new RgwRealm();
    if (this.realmData.action === 'create') {
      this.realm.name = values['realmName'];
      this.rgwRealmService.create(this.realm, values['default_realm']).subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Realm: '${values['realmName']}' created successfully`
          );
          this.closeModal();
        },
        () => {
          this.multisiteRealmForm.setErrors({ cdSubmitButton: true });
        }
      );
    } else if (this.realmData.action === 'edit') {
      this.realm.name = this.realmData.info.data.name;
      this.newRealmName = values['realmName'];
      this.rgwRealmService.update(this.realm, values['default_realm'], this.newRealmName).subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Realm: '${values['realmName']}' updated successfully`
          );
          this.closeModal();
        },
        () => {
          this.multisiteRealmForm.setErrors({ cdSubmitButton: true });
        }
      );
    }
  }
}
