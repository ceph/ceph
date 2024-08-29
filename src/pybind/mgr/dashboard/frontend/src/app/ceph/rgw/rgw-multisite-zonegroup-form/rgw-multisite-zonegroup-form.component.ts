import { Component, Inject, OnInit, Optional } from '@angular/core';
import {
  UntypedFormArray,
  UntypedFormBuilder,
  UntypedFormControl,
  NgForm,
  Validators
} from '@angular/forms';
import _ from 'lodash';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm, RgwZone, RgwZonegroup } from '../models/rgw-multisite';
import { Icons } from '~/app/shared/enum/icons.enum';
import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-rgw-multisite-zonegroup-form',
  templateUrl: './rgw-multisite-zonegroup-form.component.html',
  styleUrls: ['./rgw-multisite-zonegroup-form.component.scss']
})
export class RgwMultisiteZonegroupFormComponent extends BaseModal implements OnInit {
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,4}$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;
  icons = Icons;
  multisiteZonegroupForm: CdFormGroup;
  realm: RgwRealm;
  zonegroup: RgwZonegroup;
  realmList: RgwRealm[] = [];
  zonegroupList: RgwZonegroup[] = [];
  zonegroupNames: string[];
  isMaster = false;
  placementTargets: UntypedFormArray;
  newZonegroupName: string;
  zonegroupZoneNames: string[];
  zoneList: RgwZone[] = [];
  allZoneNames: string[];
  zgZoneNames: string[];
  zgZoneIds: string[];
  removedZones: string[];
  isRemoveMasterZone = false;
  addedZones: string[];
  disableDefault = false;
  disableMaster = false;

  constructor(
    public actionLabels: ActionLabelsI18n,
    public rgwZonegroupService: RgwZonegroupService,
    public notificationService: NotificationService,
    private formBuilder: UntypedFormBuilder,

    @Optional() @Inject('action') public action: string,
    @Optional() @Inject('resource') public resource: string,
    @Optional() @Inject('info') public info: any,
    @Optional() @Inject('multisiteInfo') public multisiteInfo: object[],
    @Optional() @Inject('defaultsInfo') public defaultsInfo: string[],
    @Optional() @Inject('editing') public editing: boolean
  ) {
    super();
    this.action = this.editing
      ? this.actionLabels.EDIT
      : this.actionLabels.CREATE;
    this.createForm();
  }

  createForm() {
    this.multisiteZonegroupForm = new CdFormGroup({
      default_zonegroup: new UntypedFormControl(false),
      zonegroupName: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (zonegroupName: string) => {
            return (
              this.action === this.actionLabels.CREATE &&
              this.zonegroupNames &&
              this.zonegroupNames.indexOf(zonegroupName) !== -1
            );
          })
        ]
      }),
      master_zonegroup: new UntypedFormControl(false),
      selectedRealm: new UntypedFormControl(null),
      zonegroup_endpoints: new UntypedFormControl(null, [
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
      ]),
      placementTargets: this.formBuilder.array([]),
      zones: new UntypedFormControl([]),
    });
  }

  ngOnInit(): void {
    _.forEach(this.multisiteZonegroupForm.get('placementTargets'), (placementTarget) => {
      const fg = this.addPlacementTarget();
      fg.patchValue(placementTarget);
    });
    this.placementTargets = this.multisiteZonegroupForm.get('placementTargets') as UntypedFormArray;
    this.realmList =
      this.multisiteInfo[0] !== undefined && this.multisiteInfo[0].hasOwnProperty('realms')
        ? this.multisiteInfo[0]['realms']
        : [];
    this.zonegroupList =
      this.multisiteInfo[1] !== undefined && this.multisiteInfo[1].hasOwnProperty('zonegroups')
        ? this.multisiteInfo[1]['zonegroups']
        : [];
    this.zonegroupList.forEach((zgp: any) => {
      if (zgp.is_master === true && !_.isEmpty(zgp.realm_id)) {
        this.isMaster = true;
        this.disableMaster = true;
      }
    });
    if (!this.isMaster) {
      this.multisiteZonegroupForm.get('master_zonegroup').setValue(true);
      this.multisiteZonegroupForm.get('master_zonegroup').disable();
    }
    this.zoneList =
      this.multisiteInfo[2] !== undefined && this.multisiteInfo[2].hasOwnProperty('zones')
        ? this.multisiteInfo[2]['zones']
        : [];
    this.zonegroupNames = this.zonegroupList.map((zonegroup) => {
      return zonegroup['name'];
    });
    let allZonegroupZonesList = this.zonegroupList.map((zonegroup: RgwZonegroup) => {
      return zonegroup['zones'];
    });
    const allZonegroupZonesInfo = allZonegroupZonesList.reduce(
      (accumulator, value) => accumulator.concat(value),
      []
    );
    const allZonegroupZonesNames = allZonegroupZonesInfo.map((zone) => {
      return zone['name'];
    });
    this.allZoneNames = this.zoneList.map((zone: RgwZone) => {
      return zone['name'];
    });
    this.allZoneNames = _.difference(this.allZoneNames, allZonegroupZonesNames);
    if (this.action === this.actionLabels.CREATE && this.defaultsInfo['defaultRealmName'] !== null) {
      this.multisiteZonegroupForm
        .get('selectedRealm')
        .setValue(this.defaultsInfo['defaultRealmName']);
      if (this.disableMaster) {
        this.multisiteZonegroupForm.get('master_zonegroup').disable();
      }
    }
    if (this.action === this.actionLabels.EDIT) {
      this.multisiteZonegroupForm.get('zonegroupName').setValue(this.info.data.name);
      this.multisiteZonegroupForm.get('selectedRealm').setValue(this.info.data.parent);
      this.multisiteZonegroupForm.get('default_zonegroup').setValue(this.info.data.is_default);
      this.multisiteZonegroupForm.get('master_zonegroup').setValue(this.info.data.is_master);
      this.multisiteZonegroupForm.get('zonegroup_endpoints').setValue(this.info.data.endpoints);

      if (this.info.data.is_default) {
        this.multisiteZonegroupForm.get('default_zonegroup').disable();
      }
      if (
        !this.info.data.is_default &&
        this.multisiteZonegroupForm.getValue('selectedRealm') !==
          this.defaultsInfo['defaultRealmName']
      ) {
        this.multisiteZonegroupForm.get('default_zonegroup').disable();
        this.disableDefault = true;
      }
      if (this.info.data.is_master || this.disableMaster) {
        this.multisiteZonegroupForm.get('master_zonegroup').disable();
      }

      this.zonegroupZoneNames = this.info.data.zones.map((zone: { [x: string]: any }) => {
        return { content: zone['name'], selected: true };
      });
      this.zgZoneNames = this.info.data.zones.map((zone: { [x: string]: any }) => {
        return zone['name'];
      });
      this.zgZoneIds = this.info.data.zones.map((zone: { [x: string]: any }) => {
        return zone['id'];
      });

      this.info.data.placement_targets.forEach((target: object) => {
        const fg = this.addPlacementTarget();
        let data = {
          placement_id: target['name'],
          tags: target['tags'].join(','),
          storage_class:
            typeof target['storage_classes'] === 'string'
              ? target['storage_classes']
              : target['storage_classes'].join(',')
        };
        fg.patchValue(data);
      });
    }
  }

  submit() {
    const values = this.multisiteZonegroupForm.getRawValue();
    if (this.action === this.actionLabels.CREATE) {
      this.realm = new RgwRealm();
      this.realm.name = values['selectedRealm'];
      this.zonegroup = new RgwZonegroup();
      this.zonegroup.name = values['zonegroupName'];
      this.zonegroup.endpoints = values['zonegroup_endpoints'];
      this.rgwZonegroupService
        .create(this.realm, this.zonegroup, values['default_zonegroup'], values['master_zonegroup'])
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Zonegroup: '${values['zonegroupName']}' created successfully`
            );
            this.closeModal();
          },
          () => {
            this.multisiteZonegroupForm.setErrors({ cdSubmitButton: true });
          }
        );
    } else if (this.action === this.actionLabels.EDIT) {
      const masterZoneName = this.info.data.zones.filter(
        (zone: any) => zone.id === this.info.data.master_zone
      );
      if (!this.removedZones)
        this.removedZones = _.difference(this.zgZoneNames, this.zonegroupZoneNames);
      this.isRemoveMasterZone = this.removedZones.includes(masterZoneName[0].name);
      if (this.isRemoveMasterZone) {
        this.multisiteZonegroupForm.setErrors({ cdSubmitButton: true });
        return;
      }
      if (!this.addedZones)
        this.addedZones = _.difference(this.zonegroupZoneNames, this.zgZoneNames);
      this.realm = new RgwRealm();
      this.realm.name = values['selectedRealm'];
      this.zonegroup = new RgwZonegroup();
      this.zonegroup.name = this.info.data.name;
      this.newZonegroupName = values['zonegroupName'];
      this.zonegroup.endpoints = values['zonegroup_endpoints'].toString();
      this.zonegroup.placement_targets = values['placementTargets'];
      this.rgwZonegroupService
        .update(
          this.realm,
          this.zonegroup,
          this.newZonegroupName,
          values['default_zonegroup'],
          values['master_zonegroup'],
          this.removedZones,
          this.addedZones
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Zonegroup: '${values['zonegroupName']}' updated successfully`
            );
            this.closeModal();
          },
          () => {
            this.multisiteZonegroupForm.setErrors({ cdSubmitButton: true });
          }
        );
    }
  }

  zoneSelectionChangeHandler(event: any) {
    this.addedZones = event.map((zone: any) => zone.content);
    this.removedZones = _.difference(this.zgZoneNames, this.addedZones);
  }

  addPlacementTarget() {
    this.placementTargets = this.multisiteZonegroupForm.get('placementTargets') as UntypedFormArray;
    const fg = new CdFormGroup({
      placement_id: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      tags: new UntypedFormControl(''),
      storage_class: new UntypedFormControl([])
    });
    this.placementTargets.push(fg);
    return fg;
  }

  trackByFn(index: number) {
    return index;
  }

  removePlacementTarget(index: number) {
    this.placementTargets = this.multisiteZonegroupForm.get('placementTargets') as UntypedFormArray;
    this.placementTargets.removeAt(index);
  }

  showError(index: number, control: string, formDir: NgForm, x: string) {
    return (<any>this.multisiteZonegroupForm.controls.placementTargets).controls[index].showError(
      control,
      formDir,
      x
    );
  }
}
