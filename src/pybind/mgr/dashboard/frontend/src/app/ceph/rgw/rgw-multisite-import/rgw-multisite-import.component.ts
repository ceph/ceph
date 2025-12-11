import { Component, OnInit } from '@angular/core';
import { BaseModal } from 'carbon-components-angular';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { ComboBoxItem } from '~/app/shared/models/combo-box.model';
import { HostService } from '~/app/shared/api/host.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwZone } from '../models/rgw-multisite';
import _ from 'lodash';

@Component({
  selector: 'cd-rgw-multisite-import',
  templateUrl: './rgw-multisite-import.component.html',
  styleUrls: ['./rgw-multisite-import.component.scss']
})
export class RgwMultisiteImportComponent extends BaseModal implements OnInit {
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,4}$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;
  importTokenForm: CdFormGroup;
  multisiteInfo: object[] = [];
  zoneList: RgwZone[] = [];
  zoneNames: string[];
  hosts: ComboBoxItem[] = [];
  labels: ComboBoxItem[] = [];
  selectedHosts: string[] = [];

  INVALID_TEXTS = {
    required: $localize`This field is required.`,
    uniqueName: $localize`The chosen zone name is already in use.`,
    min: $localize`The value must be at least 1.`,
    max: $localize`The value cannot exceed 65535.`,
    pattern: $localize`The entered value needs to be a number.`
  };

  constructor(
    public activeModal: NgbActiveModal,
    public hostService: HostService,

    public rgwRealmService: RgwRealmService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService
  ) {
    super();
    this.createForm();
  }
  ngOnInit(): void {
    this.zoneList =
      this.multisiteInfo[2] !== undefined && this.multisiteInfo[2].hasOwnProperty('zones')
        ? this.multisiteInfo[2]['zones']
        : [];
    this.zoneNames = this.zoneList.map((zone) => {
      return zone['name'];
    });
    this.hostService.getAllHosts().subscribe((resp: object[]) => {
      const options: ComboBoxItem[] = [];
      _.forEach(resp, (host: object) => {
        if (_.get(host, 'sources.orchestrator', false)) {
          const hostname = _.get(host, 'hostname');
          options.push({ content: hostname, selected: false, name: hostname });
        }
      });

      this.hosts = [...options];
    });
    this.hostService.getLabels().subscribe((resp: string[]) => {
      this.labels = resp.map((label) => {
        return {
          content: label,
          selected: false,
          name: label
        };
      });
    });
  }

  createForm() {
    this.importTokenForm = new CdFormGroup({
      realmToken: new FormControl('', {
        validators: [Validators.required]
      }),
      zoneName: new FormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (zoneName: string) => {
            return this.zoneNames && this.zoneNames.indexOf(zoneName) !== -1;
          })
        ]
      }),
      rgw_frontend_port: new FormControl(null, {
        validators: [
          CdValidators.requiredIf({
            unmanaged: false
          }),
          Validators.pattern('^[0-9]*$')
        ]
      }),
      placement: new FormControl('hosts'),
      label: new FormControl(null, [
        CdValidators.requiredIf({
          placement: 'label',
          unmanaged: false
        })
      ]),
      hosts: new FormControl([]),
      count: new FormControl(null, [CdValidators.number(false)]),
      unmanaged: new FormControl(false),
      archive_zone: new FormControl(false)
    });
  }

  multiSelector(event: ComboBoxItem[]) {
    this.selectedHosts = event.map((host: ComboBoxItem) => host.content);
  }

  onSubmit() {
    const values = this.importTokenForm.value;
    const placementSpec: object = {
      placement: {}
    };
    const tier_type: string = values['archive_zone'] ? 'archive' : '';
    if (!values['unmanaged']) {
      switch (values['placement']) {
        case 'hosts':
          if (this.selectedHosts.length > 0) {
            placementSpec['placement']['hosts'] = this.selectedHosts;
          }
          break;
        case 'label':
          if (!_.isEmpty(values['label'])) {
            placementSpec['placement']['label'] = values['label']?.content;
          }
          break;
      }
      if (_.isNumber(values['count']) && values['count'] > 0) {
        placementSpec['placement']['count'] = values['count'];
      }
    }

    this.rgwRealmService
      .importRealmToken(
        values['realmToken'],
        values['zoneName'],
        values['rgw_frontend_port'],
        placementSpec,
        tier_type
      )
      .subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Realm token import successfull`
          );
          this.closeModal();
        },
        () => {
          this.importTokenForm.setErrors({ cdSubmitButton: true });
        }
      );
  }
}
