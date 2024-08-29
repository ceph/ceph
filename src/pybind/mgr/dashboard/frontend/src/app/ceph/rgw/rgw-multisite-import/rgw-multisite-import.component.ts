import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwZone } from '../models/rgw-multisite';
import _ from 'lodash';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { HostService } from '~/app/shared/api/host.service';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { Observable, Subject, forkJoin } from 'rxjs';
import { map } from 'rxjs/operators';
import { BaseModal } from 'carbon-components-angular';

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
  hosts: any;
  labels: string[];
  labelClick = new Subject<string>();
  labelFocus = new Subject<string>();
  hostsAndLabels$: Observable<{ hosts: any[]; labels: any[] }>;

  selectedLabels: string[] = [];
  selectedHosts: string[] = [];

  constructor(
    public hostService: HostService,

    public rgwRealmService: RgwRealmService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService
  ) {
    super();
    this.hosts = {
      options: [],
      messages: new SelectMessages({
        empty: $localize`There are no hosts.`,
        filter: $localize`Filter hosts`
      })
    };
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
    const hostContext = new CdTableFetchDataContext(() => undefined);
    this.hostsAndLabels$ = forkJoin({
      hosts: this.hostService.list(hostContext.toParams(), 'false'),
      labels: this.hostService.getLabels()
    }).pipe(
      map(({ hosts, labels }) => ({
        hosts: hosts.map((host: any) => ({ content: host['hostname'] })),
        labels: labels.map((label: string) => ({ content: label }))
      }))
    );
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
        validators: [Validators.required, Validators.pattern('^[0-9]*$')]
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
      unmanaged: new FormControl(false)
    });
  }

  multiSelector(event: any, field: 'label' | 'hosts') {
    if (field === 'label') this.selectedLabels = event.map((label: any) => label.content);
    else this.selectedHosts = event.map((host: any) => host.content);
  }

  onSubmit() {
    const values = this.importTokenForm.value;
    const placementSpec: object = {
      placement: {}
    };
    if (!values['unmanaged']) {
      switch (values['placement']) {
        case 'hosts':
          if (values['hosts'].length > 0) {
            placementSpec['placement']['hosts'] = this.selectedHosts;
          }
          break;
        case 'label':
          placementSpec['placement']['label'] = this.selectedLabels;
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
        placementSpec
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
