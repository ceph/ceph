import { Component, EventEmitter, Output } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm, RgwZonegroup, RgwZone, SystemKey } from '../models/rgw-multisite';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Subscription } from 'rxjs';
import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-create-rgw-service-entities',
  templateUrl: './create-rgw-service-entities.component.html',
  styleUrls: ['./create-rgw-service-entities.component.scss']
})
export class CreateRgwServiceEntitiesComponent extends BaseModal {
  public sub = new Subscription();
  createMultisiteEntitiesForm: CdFormGroup;
  realm: RgwRealm;
  zonegroup: RgwZonegroup;
  zone: RgwZone;

  @Output()
  submitAction = new EventEmitter();

  constructor(
    public actionLabels: ActionLabelsI18n,
    public rgwMultisiteService: RgwMultisiteService,
    public rgwZoneService: RgwZoneService,
    public notificationService: NotificationService,
    public rgwZonegroupService: RgwZonegroupService,
    public rgwRealmService: RgwRealmService,
  ) {
    super();
    this.createForm();
  }

  createForm() {
    this.createMultisiteEntitiesForm = new CdFormGroup({
      realmName: new FormControl(null, {
        validators: [Validators.required]
      }),
      zonegroupName: new FormControl(null, {
        validators: [Validators.required]
      }),
      zoneName: new FormControl(null, {
        validators: [Validators.required]
      })
    });
  }

  submit() {
    const values = this.createMultisiteEntitiesForm.value;
    this.realm = new RgwRealm();
    this.realm.name = values['realmName'];
    this.zonegroup = new RgwZonegroup();
    this.zonegroup.name = values['zonegroupName'];
    this.zonegroup.endpoints = '';
    this.zone = new RgwZone();
    this.zone.name = values['zoneName'];
    this.zone.endpoints = '';
    this.zone.system_key = new SystemKey();
    this.zone.system_key.access_key = '';
    this.zone.system_key.secret_key = '';
    this.rgwRealmService
      .create(this.realm, true)
      .toPromise()
      .then(() => {
        this.rgwZonegroupService
          .create(this.realm, this.zonegroup, true, true)
          .toPromise()
          .then(() => {
            this.rgwZoneService
              .create(this.zone, this.zonegroup, true, true, this.zone.endpoints)
              .toPromise()
              .then(() => {
                this.notificationService.show(
                  NotificationType.success,
                  $localize`Realm/Zonegroup/Zone created successfully`
                );
                this.submitAction.emit();
                this.closeModal();
              })
              .catch(() => {
                this.notificationService.show(
                  NotificationType.error,
                  $localize`Realm/Zonegroup/Zone creation failed`
                );
              });
          });
      });
  }
}
