import { Component, Inject, OnInit, Optional } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm, RgwZone, RgwZonegroup, SystemKey } from '../models/rgw-multisite';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { CdForm } from '~/app/shared/forms/cd-form';

@Component({
  selector: 'cd-rgw-multisite-zone-form',
  templateUrl: './rgw-multisite-zone-form.component.html',
  styleUrls: ['./rgw-multisite-zone-form.component.scss']
})
export class RgwMultisiteZoneFormComponent extends CdForm implements OnInit {
  multisiteZoneForm: CdFormGroup;
  realm: RgwRealm;
  zonegroup: RgwZonegroup;
  zone: RgwZone;
  zonegroupList: RgwZonegroup[] = [];
  zoneList: RgwZone[] = [];
  zoneNames: string[];
  users: any;
  placementTargets: any;
  zoneInfo: RgwZone;
  poolList: object[] = [];
  storageClassList: object[] = [];
  disableDefault: boolean = false;
  disableMaster: boolean = false;
  isMetadataSync: boolean = false;
  isMasterZone: boolean;
  isDefaultZone: boolean;
  syncStatusTimedOut: boolean = false;
  bsModalRef: NgbModalRef;
  createSystemUser: boolean = false;
  master_zone_of_master_zonegroup: RgwZone;
  masterZoneUser: any;
  access_key: any;
  master_zonegroup_of_realm: RgwZonegroup;
  compressionTypes = ['lz4', 'zlib', 'snappy'];
  userListReady: boolean = false;
  SecretKeyText =
    'To see or copy your S3 secret key, go to Object Gateway > Users and click on your user name. In Keys, click Show. View the secret key by clicking Show and copy the key by clicking Copy to Clipboard';
  AccessKeyText =
    'To see or copy your S3 access key, go to Object Gateway > Users and click on your user name. In Keys, click Show. View the access key by clicking Show and copy the key by clicking Copy to Clipboard';
  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwMultisiteService: RgwMultisiteService,
    public rgwZoneService: RgwZoneService,
    public rgwZoneGroupService: RgwZonegroupService,
    public notificationService: NotificationService,
    public rgwUserService: RgwUserService,
    public modalService: ModalCdsService,
    @Optional() @Inject('resource') public resource: string,
    @Optional() @Inject('defaultsInfo') public defaultsInfo: string[],
    @Optional() @Inject('multisiteInfo') public multisiteInfo: object[],
    @Optional() @Inject('action') public action: string,
    @Optional() @Inject('info') public info: any
  ) {
    super();
    this.createForm();
  }

  createForm() {
    this.multisiteZoneForm = new CdFormGroup({
      zoneName: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (zoneName: string) => {
            return (
              this.action === this.actionLabels.CREATE &&
              this.zoneNames &&
              this.zoneNames.indexOf(zoneName) !== -1
            );
          })
        ]
      }),
      default_zone: new UntypedFormControl(false),
      master_zone: new UntypedFormControl(false),
      selectedZonegroup: new UntypedFormControl(null),
      zone_endpoints: new UntypedFormControl(null, {
        validators: [CdValidators.url, Validators.required]
      }),
      access_key: new UntypedFormControl('', {}),
      secret_key: new UntypedFormControl('', {}),
      placementTarget: new UntypedFormControl(null),
      placementDataPool: new UntypedFormControl(''),
      placementIndexPool: new UntypedFormControl(null),
      placementDataExtraPool: new UntypedFormControl(null),
      storageClass: new UntypedFormControl(null),
      storageDataPool: new UntypedFormControl(null),
      storageCompression: new UntypedFormControl(null)
    });
  }

  onZoneGroupChange(zonegroupName: string) {
    let zg = new RgwZonegroup();
    zg.name = zonegroupName;
    this.rgwZoneGroupService.get(zg).subscribe((zonegroup: RgwZonegroup) => {
      if (_.isEmpty(zonegroup.master_zone)) {
        this.multisiteZoneForm.get('master_zone').setValue(true);
        this.multisiteZoneForm.get('master_zone').disable();
        this.disableMaster = false;
      } else if (!_.isEmpty(zonegroup.master_zone) && this.action === this.actionLabels.CREATE) {
        this.multisiteZoneForm.get('master_zone').setValue(false);
        this.multisiteZoneForm.get('master_zone').disable();
        this.disableMaster = true;
      }
    });
    if (
      this.multisiteZoneForm.getValue('selectedZonegroup') !==
      this.defaultsInfo['defaultZonegroupName']
    ) {
      this.disableDefault = true;
      this.multisiteZoneForm.get('default_zone').disable();
    }
  }

  ngOnInit(): void {
    this.createForm();
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
    if (this.action === this.actionLabels.CREATE) {
      if (this.defaultsInfo['defaultZonegroupName'] !== undefined) {
        this.multisiteZoneForm
          .get('selectedZonegroup')
          .setValue(this.defaultsInfo['defaultZonegroupName']);
        this.onZoneGroupChange(this.defaultsInfo['defaultZonegroupName']);
      }
    }
    if (this.action === this.actionLabels.EDIT) {
      this.placementTargets =
        this.info.data?.parent || this.info.parent
          ? (this.info.data?.parentNode || this.info.parent.data)?.placement_targets
          : [];
      this.rgwZoneService.getPoolNames().subscribe((pools: object[]) => {
        this.poolList = pools;
      });
      this.multisiteZoneForm.get('zoneName').setValue(this.info.data.name);
      this.multisiteZoneForm.get('selectedZonegroup').setValue(this.info.data.parent);
      this.multisiteZoneForm.get('default_zone').setValue(this.info.data.is_default);
      this.multisiteZoneForm.get('master_zone').setValue(this.info.data.is_master);
      this.multisiteZoneForm.get('zone_endpoints').setValue(this.info.data.endpoints.toString());
      this.multisiteZoneForm.get('access_key').setValue(this.info.data.access_key);
      this.multisiteZoneForm.get('secret_key').setValue(this.info.data.secret_key);
      this.multisiteZoneForm
        .get('placementTarget')
        .setValue((this.info.data?.parentNode || this.info.parent.data)?.default_placement);
      this.getZonePlacementData(this.multisiteZoneForm.getValue('placementTarget'));
      if (this.info.data.is_default) {
        this.isDefaultZone = true;
        this.multisiteZoneForm.get('default_zone').disable();
      }
      if (this.info.data.is_master) {
        this.isMasterZone = true;
        this.multisiteZoneForm.get('master_zone').disable();
      }
      const zone = new RgwZone();
      zone.name = this.info.data.name;
      this.onZoneGroupChange(this.info.data.parent);
    }
    if (
      this.multisiteZoneForm.getValue('selectedZonegroup') !==
      this.defaultsInfo['defaultZonegroupName']
    ) {
      this.disableDefault = true;
      this.multisiteZoneForm.get('default_zone').disable();
    }
  }

  getZonePlacementData(placementTarget: string) {
    this.zone = new RgwZone();
    this.zone.name = this.info.data.name;
    this.rgwZoneService.get(this.zone).subscribe((zoneInfo: RgwZone) => {
      this.zoneInfo = zoneInfo;
      if (this.zoneInfo && this.zoneInfo['placement_pools']) {
        const placementPoolKeys = this.zoneInfo['placement_pools'].map((plc_pool) => plc_pool.key);
        this.placementTargets = this.placementTargets.filter((placement: { name: string }) =>
          placementPoolKeys.includes(placement.name)
        );
        this.zoneInfo['placement_pools'].forEach((plc_pool) => {
          if (plc_pool.key === placementTarget) {
            let storageClasses = plc_pool.val.storage_classes;
            this.storageClassList = Object.entries(storageClasses).map(([key, value]) => ({
              key,
              value
            }));
            let placementIndexPool = plc_pool.val.index_pool;
            let placementDataExtraPool = plc_pool.val.data_extra_pool;
            this.poolList.push({ poolname: placementIndexPool });
            this.poolList.push({ poolname: placementDataExtraPool });
            this.multisiteZoneForm.get('storageClass').setValue(this.storageClassList[0]['key']);
            this.getStorageClassData(this.storageClassList[0]['key']);
            this.multisiteZoneForm.get('placementIndexPool').setValue(placementIndexPool);
            this.multisiteZoneForm.get('placementDataExtraPool').setValue(placementDataExtraPool);
          }
        });
      }
    });
  }

  getStorageClassData(storageClass: string) {
    let storageClassSelected = this.storageClassList.find((sc) => sc['key'] === storageClass);
    let dataPoolName = storageClassSelected['value']['data_pool'];
    this.poolList.push({ poolname: dataPoolName });
    this.multisiteZoneForm.get('storageDataPool').setValue(dataPoolName);
    this.multisiteZoneForm.get('placementDataPool').setValue(dataPoolName);
    this.multisiteZoneForm
      .get('storageCompression')
      .setValue(storageClassSelected['value']['compression_type']);
  }

  submit() {
    const values = this.multisiteZoneForm.getRawValue();
    if (this.action === this.actionLabels.CREATE) {
      this.zonegroup = new RgwZonegroup();
      this.zonegroup.name = values['selectedZonegroup'];
      this.zone = new RgwZone();
      this.zone.name = values['zoneName'];
      this.zone.endpoints = values['zone_endpoints'];
      this.zone.system_key = new SystemKey();
      this.zone.system_key.access_key = values['access_key'];
      this.zone.system_key.secret_key = values['secret_key'];
      this.rgwZoneService
        .create(
          this.zone,
          this.zonegroup,
          values['default_zone'],
          values['master_zone'],
          this.zone.endpoints
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Zone: '${values['zoneName']}' created successfully`
            );
            this.closeModal();
          },
          () => {
            this.multisiteZoneForm.setErrors({ cdSubmitButton: true });
          }
        );
    } else if (this.action === this.actionLabels.EDIT) {
      this.zonegroup = new RgwZonegroup();
      this.zonegroup.name = values['selectedZonegroup'];
      this.zone = new RgwZone();
      this.zone.name = this.info.data.name;
      this.zone.endpoints = values['zone_endpoints'];
      this.zone.system_key = new SystemKey();
      this.zone.system_key.access_key = values['access_key'];
      this.zone.system_key.secret_key = values['secret_key'];
      this.rgwZoneService
        .update(
          this.zone,
          this.zonegroup,
          values['zoneName'],
          values['default_zone'],
          values['master_zone'],
          this.zone.endpoints,
          values['placementTarget'],
          values['placementDataPool'],
          values['placementIndexPool'],
          values['placementDataExtraPool'],
          values['storageClass'],
          values['storageDataPool'],
          values['storageCompression']
        )
        .subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Zone: '${values['zoneName']}' updated successfully`
            );
            this.closeModal();
          },
          () => {
            this.multisiteZoneForm.setErrors({ cdSubmitButton: true });
          }
        );
    }
  }
}
