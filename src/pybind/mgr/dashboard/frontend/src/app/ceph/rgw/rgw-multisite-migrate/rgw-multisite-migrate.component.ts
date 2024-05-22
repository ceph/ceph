import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm, RgwZone, RgwZonegroup, SystemKey } from '../models/rgw-multisite';
import { ModalService } from '~/app/shared/services/modal.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';

@Component({
  selector: 'cd-rgw-multisite-migrate',
  templateUrl: './rgw-multisite-migrate.component.html',
  styleUrls: ['./rgw-multisite-migrate.component.scss']
})
export class RgwMultisiteMigrateComponent implements OnInit {
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,4}$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;

  @Output()
  submitAction = new EventEmitter();

  multisiteMigrateForm: CdFormGroup;
  zoneNames: string[];
  realmList: RgwRealm[];
  multisiteInfo: object[] = [];
  realmNames: string[];
  zonegroupList: RgwZonegroup[];
  zonegroupNames: string[];
  zoneList: RgwZone[];
  realm: RgwRealm;
  zonegroup: RgwZonegroup;
  zone: RgwZone;
  newZonegroupName: any;
  newZoneName: any;
  bsModalRef: NgbModalRef;
  users: any;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwMultisiteService: RgwMultisiteService,
    public rgwZoneService: RgwZoneService,
    public notificationService: NotificationService,
    public rgwZonegroupService: RgwZonegroupService,
    public rgwRealmService: RgwRealmService,
    public rgwDaemonService: RgwDaemonService,
    public modalService: ModalService
  ) {
    this.createForm();
  }

  createForm() {
    this.multisiteMigrateForm = new CdFormGroup({
      realmName: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (realmName: string) => {
            return this.realmNames && this.zoneNames.indexOf(realmName) !== -1;
          })
        ]
      }),
      zonegroupName: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (zonegroupName: string) => {
            return this.zonegroupNames && this.zoneNames.indexOf(zonegroupName) !== -1;
          })
        ]
      }),
      zoneName: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (zoneName: string) => {
            return this.zoneNames && this.zoneNames.indexOf(zoneName) !== -1;
          })
        ]
      }),
      zone_endpoints: new UntypedFormControl([], {
        validators: [
          CdValidators.custom('endpoint', (value: string) => {
            if (_.isEmpty(value)) {
              return false;
            } else {
              if (value.includes(',')) {
                value.split(',').forEach((url: string) => {
                  return (
                    !this.endpoints.test(url) && !this.ipv4Rgx.test(url) && !this.ipv6Rgx.test(url)
                  );
                });
              } else {
                return (
                  !this.endpoints.test(value) &&
                  !this.ipv4Rgx.test(value) &&
                  !this.ipv6Rgx.test(value)
                );
              }
              return false;
            }
          }),
          Validators.required
        ]
      }),
      zonegroup_endpoints: new UntypedFormControl(
        [],
        [
          CdValidators.custom('endpoint', (value: string) => {
            if (_.isEmpty(value)) {
              return false;
            } else {
              if (value.includes(',')) {
                value.split(',').forEach((url: string) => {
                  return (
                    !this.endpoints.test(url) && !this.ipv4Rgx.test(url) && !this.ipv6Rgx.test(url)
                  );
                });
              } else {
                return (
                  !this.endpoints.test(value) &&
                  !this.ipv4Rgx.test(value) &&
                  !this.ipv6Rgx.test(value)
                );
              }
              return false;
            }
          }),
          Validators.required
        ]
      ),
      access_key: new UntypedFormControl(null),
      secret_key: new UntypedFormControl(null)
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
    this.zonegroupList =
      this.multisiteInfo[1] !== undefined && this.multisiteInfo[1].hasOwnProperty('zonegroups')
        ? this.multisiteInfo[1]['zonegroups']
        : [];
    this.zonegroupNames = this.zonegroupList.map((zonegroup) => {
      return zonegroup['name'];
    });
    this.zoneList =
      this.multisiteInfo[2] !== undefined && this.multisiteInfo[2].hasOwnProperty('zones')
        ? this.multisiteInfo[2]['zones']
        : [];
    this.zoneNames = this.zoneList.map((zone) => {
      return zone['name'];
    });
  }

  submit() {
    const values = this.multisiteMigrateForm.value;
    this.realm = new RgwRealm();
    this.realm.name = values['realmName'];
    this.zonegroup = new RgwZonegroup();
    this.zonegroup.name = values['zonegroupName'];
    this.zonegroup.endpoints = values['zonegroup_endpoints'];
    this.zone = new RgwZone();
    this.zone.name = values['zoneName'];
    this.zone.endpoints = values['zone_endpoints'];
    this.zone.system_key = new SystemKey();
    this.zone.system_key.access_key = values['access_key'];
    this.zone.system_key.secret_key = values['secret_key'];
    this.rgwMultisiteService.migrate(this.realm, this.zonegroup, this.zone).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Migration done successfully`
        );
        this.submitAction.emit();
        this.activeModal.close();
      },
      () => {
        this.notificationService.show(NotificationType.error, $localize`Migration failed`);
      }
    );
  }
}
