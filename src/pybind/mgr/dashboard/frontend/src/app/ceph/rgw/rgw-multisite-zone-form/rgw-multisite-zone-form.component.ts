import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm, RgwZone, RgwZonegroup } from '../models/rgw-multisite';

@Component({
  selector: 'cd-rgw-multisite-zone-form',
  templateUrl: './rgw-multisite-zone-form.component.html',
  styleUrls: ['./rgw-multisite-zone-form.component.scss']
})
export class RgwMultisiteZoneFormComponent implements OnInit {
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,4}$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;
  action: string;
  multisiteZoneForm: CdFormGroup;
  editing = false;
  resource: string;
  realm: RgwRealm;
  zonegroup: RgwZonegroup;
  zone: RgwZone;
  defaultsInfo: string[] = [];
  multisiteInfo: object[] = [];
  zonegroupList: RgwZonegroup[] = [];
  zoneList: RgwZone[] = [];
  zoneNames: string[];
  users: string[];

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwZoneService: RgwZoneService,
    public notificationService: NotificationService,
    public rgwUserService: RgwUserService
  ) {
    this.action = this.editing
      ? this.actionLabels.EDIT + this.resource
      : this.actionLabels.CREATE + this.resource;
    this.createForm();
  }

  createForm() {
    this.multisiteZoneForm = new CdFormGroup({
      zoneName: new FormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (zoneName: string) => {
            return this.zoneNames && this.zoneNames.indexOf(zoneName) !== -1;
          })
        ]
      }),
      default_zone: new FormControl(false),
      master_zone: new FormControl(false),
      selectedZonegroup: new FormControl(null),
      zone_endpoints: new FormControl(null, {
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
          })
        ]
      }),
      users: new FormControl(null)
    });
  }

  ngOnInit(): void {
    this.zonegroupList =
      this.multisiteInfo[1] !== undefined && this.multisiteInfo[1].hasOwnProperty('zonegroups')
        ? this.multisiteInfo[1]['zonegroups']
        : [];
    this.zoneList =
      this.multisiteInfo[2] !== undefined && this.multisiteInfo[2].hasOwnProperty('zones')
        ? this.multisiteInfo[2]['zones']
        : [];
    this.zoneNames = this.zoneList.map((zone) => {
      return zone['name'];
    });
    if (this.action === 'create') {
      this.multisiteZoneForm
        .get('selectedZonegroup')
        .setValue(this.defaultsInfo['defaultZonegroupName']);
    }
    this.rgwUserService.list().subscribe((users: any) => {
      this.users = users.filter((user: any) => user.keys.length !== 0);
    });
  }

  submit() {
    const values = this.multisiteZoneForm.value;
    this.zonegroup = new RgwZonegroup();
    this.zonegroup.name = values['selectedZonegroup'];
    this.zone = new RgwZone();
    this.zone.name = values['zoneName'];
    this.rgwZoneService
      .create(
        this.zone,
        this.zonegroup,
        values['default_zone'],
        values['master_zone'],
        values['zone_endpoints'],
        values['users']
      )
      .subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Zone: '${values['zoneName']}' created successfully`
          );
          this.activeModal.close();
        },
        () => {
          this.multisiteZoneForm.setErrors({ cdSubmitButton: true });
        }
      );
  }
}
