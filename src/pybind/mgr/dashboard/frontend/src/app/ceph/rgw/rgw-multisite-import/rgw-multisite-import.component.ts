import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
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
export class RgwMultisiteImportComponent implements OnInit {
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,4}$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;

  importTokenForm: CdFormGroup;
  multisiteInfo: object[] = [];
  zoneList: RgwZone[] = [];
  zoneNames: string[];

  constructor(
    public activeModal: NgbActiveModal,
    public rgwRealmService: RgwRealmService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService
  ) {
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
      })
    });
  }

  onSubmit() {
    const values = this.importTokenForm.value;
    this.rgwRealmService.importRealmToken(values['realmToken'], values['zoneName']).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Realm token import successfull`
        );
        this.activeModal.close();
      },
      () => {
        this.importTokenForm.setErrors({ cdSubmitButton: true });
      }
    );
  }
}
