import { Component, OnInit, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal, NgbTypeahead } from '@ng-bootstrap/ng-bootstrap';
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
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import { Observable, Subject, merge } from 'rxjs';
import { debounceTime, distinctUntilChanged, filter, map } from 'rxjs/operators';

@Component({
  selector: 'cd-rgw-multisite-import',
  templateUrl: './rgw-multisite-import.component.html',
  styleUrls: ['./rgw-multisite-import.component.scss']
})
export class RgwMultisiteImportComponent implements OnInit {
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,4}$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;
  @ViewChild(NgbTypeahead, { static: false })
  typeahead: NgbTypeahead;

  importTokenForm: CdFormGroup;
  multisiteInfo: object[] = [];
  zoneList: RgwZone[] = [];
  zoneNames: string[];
  hosts: any;
  labels: string[];
  labelClick = new Subject<string>();
  labelFocus = new Subject<string>();

  constructor(
    public activeModal: NgbActiveModal,
    public hostService: HostService,

    public rgwRealmService: RgwRealmService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService
  ) {
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
    this.hostService.list(hostContext.toParams(), 'false').subscribe((resp: object[]) => {
      const options: SelectOption[] = [];
      _.forEach(resp, (host: object) => {
        if (_.get(host, 'sources.orchestrator', false)) {
          const option = new SelectOption(false, _.get(host, 'hostname'), '');
          options.push(option);
        }
      });
      this.hosts.options = [...options];
    });
    this.hostService.getLabels().subscribe((resp: string[]) => {
      this.labels = resp;
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

  onSubmit() {
    const values = this.importTokenForm.value;
    const placementSpec: object = {
      placement: {}
    };
    if (!values['unmanaged']) {
      switch (values['placement']) {
        case 'hosts':
          if (values['hosts'].length > 0) {
            placementSpec['placement']['hosts'] = values['hosts'];
          }
          break;
        case 'label':
          placementSpec['placement']['label'] = values['label'];
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
          this.activeModal.close();
        },
        () => {
          this.importTokenForm.setErrors({ cdSubmitButton: true });
        }
      );
  }

  searchLabels = (text$: Observable<string>) => {
    return merge(
      text$.pipe(debounceTime(200), distinctUntilChanged()),
      this.labelFocus,
      this.labelClick.pipe(filter(() => !this.typeahead.isPopupOpen()))
    ).pipe(
      map((value) =>
        this.labels
          .filter((label: string) => label.toLowerCase().indexOf(value.toLowerCase()) > -1)
          .slice(0, 10)
      )
    );
  };
}
