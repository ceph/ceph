import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm, RgwZonegroup } from '../models/rgw-multisite';

@Component({
  selector: 'cd-rgw-multisite-zonegroup-form',
  templateUrl: './rgw-multisite-zonegroup-form.component.html',
  styleUrls: ['./rgw-multisite-zonegroup-form.component.scss']
})
export class RgwMultisiteZonegroupFormComponent implements OnInit {
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,4}$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;
  action: string;
  multisiteZonegroupForm: CdFormGroup;
  editing = false;
  resource: string;
  realm: RgwRealm;
  zonegroup: RgwZonegroup;
  defaultsInfo: string[] = [];
  multisiteInfo: object[] = [];
  realmList: RgwRealm[] = [];
  zonegroupList: RgwZonegroup[] = [];
  zonegroupNames: string[];

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwZonegroupService: RgwZonegroupService,
    public notificationService: NotificationService
  ) {
    this.action = this.editing
      ? this.actionLabels.EDIT + this.resource
      : this.actionLabels.CREATE + this.resource;
    this.createForm();
  }

  createForm() {
    this.multisiteZonegroupForm = new CdFormGroup({
      default_zonegroup: new FormControl(false),
      zonegroupName: new FormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (zonegroupName: string) => {
            return this.zonegroupNames && this.zonegroupNames.indexOf(zonegroupName) !== -1;
          })
        ]
      }),
      master_zonegroup: new FormControl(false),
      selectedRealm: new FormControl(null),
      zonegroup_endpoints: new FormControl(null, [
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
      ])
    });
  }

  ngOnInit(): void {
    this.realmList =
      this.multisiteInfo[0] !== undefined && this.multisiteInfo[0].hasOwnProperty('realms')
        ? this.multisiteInfo[0]['realms']
        : [];
    this.zonegroupList =
      this.multisiteInfo[1] !== undefined && this.multisiteInfo[1].hasOwnProperty('zonegroups')
        ? this.multisiteInfo[1]['zonegroups']
        : [];
    this.zonegroupNames = this.zonegroupList.map((zonegroup) => {
      return zonegroup['name'];
    });
    if (this.action === 'create' && this.defaultsInfo['defaultRealmName'] !== null) {
      this.multisiteZonegroupForm
        .get('selectedRealm')
        .setValue(this.defaultsInfo['defaultRealmName']);
    }
  }

  submit() {
    const values = this.multisiteZonegroupForm.value;
    this.realm = new RgwRealm();
    this.realm.name = values['selectedRealm'];
    this.zonegroup = new RgwZonegroup();
    this.zonegroup.name = values['zonegroupName'];
    this.zonegroup.endpoints = this.checkUrlArray(values['zonegroup_endpoints']);
    this.rgwZonegroupService
      .create(this.realm, this.zonegroup, values['default_zonegroup'], values['master_zonegroup'])
      .subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Zonegroup: '${values['zonegroupName']}' created successfully`
          );
          this.activeModal.close();
        },
        () => {
          this.multisiteZonegroupForm.setErrors({ cdSubmitButton: true });
        }
      );
  }

  checkUrlArray(endpoints: string) {
    let endpointsArray = [];
    if (endpoints.includes(',')) {
      endpointsArray = endpoints.split(',');
    } else {
      endpointsArray.push(endpoints);
    }
    return endpointsArray;
  }
}
