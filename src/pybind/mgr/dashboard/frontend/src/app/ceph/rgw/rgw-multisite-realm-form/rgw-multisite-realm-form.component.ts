import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm } from '../models/rgw-multisite';

@Component({
  selector: 'cd-rgw-multisite-realm-form',
  templateUrl: './rgw-multisite-realm-form.component.html',
  styleUrls: ['./rgw-multisite-realm-form.component.scss']
})
export class RgwMultisiteRealmFormComponent implements OnInit {
  action: string;
  multisiteRealmForm: CdFormGroup;
  editing = false;
  resource: string;
  multisiteInfo: object[] = [];
  realm: RgwRealm;
  realmList: RgwRealm[] = [];
  realmNames: string[];

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwRealmService: RgwRealmService,
    public notificationService: NotificationService
  ) {
    this.action = this.editing
      ? this.actionLabels.EDIT + this.resource
      : this.actionLabels.CREATE + this.resource;
    this.createForm();
  }

  createForm() {
    this.multisiteRealmForm = new CdFormGroup({
      realmName: new FormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (realmName: string) => {
            return this.realmNames && this.realmNames.indexOf(realmName) !== -1;
          })
        ]
      }),
      default_realm: new FormControl(false)
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
  }

  submit() {
    const values = this.multisiteRealmForm.value;
    this.realm = new RgwRealm();
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
  }
}
