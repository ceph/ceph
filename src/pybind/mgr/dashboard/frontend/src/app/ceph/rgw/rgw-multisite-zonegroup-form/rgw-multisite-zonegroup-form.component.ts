import { Component, Inject, OnInit, Optional } from '@angular/core';
import { UntypedFormControl, NgForm, Validators, FormArray, FormControl } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
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
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { PlacementTarget } from '../models/rgw-storage-class.model';

@Component({
  selector: 'cd-rgw-multisite-zonegroup-form',
  templateUrl: './rgw-multisite-zonegroup-form.component.html',
  styleUrls: ['./rgw-multisite-zonegroup-form.component.scss'],
  standalone: false
})
export class RgwMultisiteZonegroupFormComponent extends BaseModal implements OnInit {
  icons = Icons;
  multisiteZonegroupForm: CdFormGroup;
  realm: RgwRealm;
  zonegroup: RgwZonegroup;
  realmList: RgwRealm[] = [];
  zonegroupList: RgwZonegroup[] = [];
  zonegroupNames: string[];
  isMaster = false;
  newZonegroupName: string;
  zonegroupZoneNames: string[] = [];
  zoneList: RgwZone[] = [];
  allZoneNames: string[];
  zgZoneNames: string[];
  zgZoneIds: string[];
  removedZones: string[];
  isRemoveMasterZone = false;
  addedZones: string[];
  disableDefault = false;
  disableMaster = false;
  masterHelperText: string;
  docUrl: string;
  INVALID_TEXTS = {
    required: 'This field is required',
    invalidURL: 'Please enter a valid URL.',
    uniqueName: 'The chosen zonegroup name is already in use.'
  };

  allPossibleValues: { content: string; value: string; selected?: boolean }[] = [];

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
    public rgwZonegroupService: RgwZonegroupService,
    public notificationService: NotificationService,
    @Optional() @Inject('action') public action: string,
    @Optional() @Inject('resource') public resource: string,
    @Optional() @Inject('info') public info: any,
    @Optional() @Inject('multisiteInfo') public multisiteInfo: any[],
    @Optional() @Inject('defaultsInfo') public defaultsInfo: Record<string, string>
  ) {
    super();
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
      zonegroup_endpoints: new UntypedFormControl(null, {
        validators: [CdValidators.url, Validators.required]
      }),
      zones: new UntypedFormControl([]),
      placementTargets: new FormArray([])
    });
  }

  get placementTargets(): FormArray {
    return this.multisiteZonegroupForm.get('placementTargets') as FormArray;
  }

  ngOnInit(): void {
    _.forEach(this.multisiteZonegroupForm.get('placementTargets'), (placementTarget) => {
      const fg = this.addPlacementTarget();
      fg.patchValue(placementTarget);
    });
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
    if (
      this.action === this.actionLabels.CREATE &&
      this.defaultsInfo['defaultRealmName'] !== null
    ) {
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
      this.multisiteZonegroupForm
        .get('zonegroup_endpoints')
        .setValue(this.info.data.endpoints.toString());

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
        return zone['name'];
      });
      this.zgZoneNames = [...this.zonegroupZoneNames];
      this.zgZoneIds = this.info.data.zones.map((zone: { [x: string]: any }) => {
        return zone['id'];
      });

      // Build allPossibleValues with existing zones (selected) and available zones (not selected)
      const existingZonesItems = this.zonegroupZoneNames.map((zone) => ({
        content: zone,
        value: zone,
        selected: true
      }));
      const availableZonesItems = this.allZoneNames.map((zone) => ({
        content: zone,
        value: zone,
        selected: false
      }));
      this.allPossibleValues = [...existingZonesItems, ...availableZonesItems];

      // Set the zones form control with the existing zone names
      this.multisiteZonegroupForm.get('zones').setValue(this.zonegroupZoneNames);

      this.info.data.placement_targets.forEach((target: PlacementTarget) => {
        const fg = this.addPlacementTarget();
        const data = {
          placement_id: target.name,
          tags: target.tags?.join(','),
          storage_class: Array.isArray(target.storage_classes)
            ? target.storage_classes.join(',')
            : target.storage_classes
        };
        fg.patchValue(data);
      });
    } else {
      this.addPlacementTarget();
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
      this.removedZones = _.difference(this.zgZoneNames, this.zonegroupZoneNames);
      const masterZoneName = this.info.data.zones.filter(
        (zone: any) => zone.id === this.info.data.master_zone
      );
      this.isRemoveMasterZone = this.removedZones.includes(masterZoneName[0].name);
      if (this.isRemoveMasterZone) {
        this.multisiteZonegroupForm.setErrors({ cdSubmitButton: true });
        return;
      }
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

  public createPlacementTarget(): CdFormGroup {
    const group = this.formBuilder.group({
      placement_id: new FormControl('', Validators.required),
      tags: new FormControl(''),
      storage_class: new FormControl([])
    });
    return group;
  }

  addPlacementTarget(): CdFormGroup {
    const group = this.createPlacementTarget();
    this.placementTargets.push(group);
    return group;
  }

  trackByFn(index: number) {
    return index;
  }

  removePlacementTarget(index: number) {
    this.placementTargets.removeAt(index);
  }

  showError(index: number, control: string, formDir: NgForm, x: string) {
    return (<any>this.multisiteZonegroupForm.controls.placementTargets).controls[index].showError(
      control,
      formDir,
      x
    );
  }

  onZoneSelection(event: any) {
    if (Array.isArray(event)) {
      this.zonegroupZoneNames = event.map((item: any) => item.value || item.content);
      this.multisiteZonegroupForm.get('zones').setValue(this.zonegroupZoneNames);
    }
  }
}
