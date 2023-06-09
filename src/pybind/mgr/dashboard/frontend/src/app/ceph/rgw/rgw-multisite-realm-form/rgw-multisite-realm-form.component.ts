import { Component, OnInit } from '@angular/core';
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

@Component({
  selector: 'cd-rgw-multisite-realm-form',
  templateUrl: './rgw-multisite-realm-form.component.html',
  styleUrls: ['./rgw-multisite-realm-form.component.scss']
})
export class RgwMultisiteRealmFormComponent implements OnInit {
  action: string;
  multisiteRealmForm: CdFormGroup;
  info: any;
  editing = false;
  resource: string;
  multisiteInfo: object[] = [];
  realm: RgwRealm;
  realmList: RgwRealm[] = [];
  zonegroupList: RgwRealm[] = [];
  realmNames: string[];
  newRealmName: string;
  isMaster: boolean;
  defaultsInfo: string[];
  defaultRealmDisabled = false;
  docUrl: string;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwRealmService: RgwRealmService,
    public notificationService: NotificationService,
    public docService: DocService
  ) {
    this.action = this.editing
      ? this.actionLabels.EDIT + this.resource
      : this.actionLabels.CREATE + this.resource;
    this.createForm();
  }

  createForm() {
    this.multisiteRealmForm = new CdFormGroup({
      realmName: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (realmName: string) => {
            return (
              this.action === 'create' &&
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
      this.multisiteInfo[0] !== undefined && this.multisiteInfo[0].hasOwnProperty('realms')
        ? this.multisiteInfo[0]['realms']
        : [];
    this.realmNames = this.realmList.map((realm) => {
      return realm['name'];
    });
    if (this.action === 'edit') {
      this.zonegroupList =
        this.multisiteInfo[1] !== undefined && this.multisiteInfo[1].hasOwnProperty('zonegroups')
          ? this.multisiteInfo[1]['zonegroups']
          : [];
      this.multisiteRealmForm.get('realmName').setValue(this.info.data.name);
      this.multisiteRealmForm.get('default_realm').setValue(this.info.data.is_default);
      if (this.info.data.is_default) {
        this.multisiteRealmForm.get('default_realm').disable();
      }
    }
    this.zonegroupList.forEach((zgp: any) => {
      if (zgp.is_master === true && zgp.realm_id === this.info.data.id) {
        this.isMaster = true;
      }
    });
    if (this.defaultsInfo && this.defaultsInfo['defaultRealmName'] !== null) {
      this.multisiteRealmForm.get('default_realm').disable();
      this.defaultRealmDisabled = true;
    }
    this.docUrl = this.docService.urlGenerator('rgw-multisite');
  }

  submit() {
    const values = this.multisiteRealmForm.getRawValue();
    this.realm = new RgwRealm();
    if (this.action === 'create') {
      this.realm.name = values['realmName'];
      this.rgwRealmService.create(this.realm, values['default_realm']).subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Realm: '${values['realmName']}' created successfully`
          );
          this.activeModal.close();
        },
        () => {
          this.multisiteRealmForm.setErrors({ cdSubmitButton: true });
        }
      );
    } else if (this.action === 'edit') {
      this.realm.name = this.info.data.name;
      this.newRealmName = values['realmName'];
      this.rgwRealmService.update(this.realm, values['default_realm'], this.newRealmName).subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Realm: '${values['realmName']}' updated successfully`
          );
          this.activeModal.close();
        },
        () => {
          this.multisiteRealmForm.setErrors({ cdSubmitButton: true });
        }
      );
    }
  }
}
