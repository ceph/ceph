import { Component, OnInit, ViewChild } from '@angular/core';
import { AbstractControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { NgbTypeahead } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';
import { merge, Observable, Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged, filter, map } from 'rxjs/operators';

import { CephServiceService } from '../../../../shared/api/ceph-service.service';
import { HostService } from '../../../../shared/api/host.service';
import { PoolService } from '../../../../shared/api/pool.service';
import { SelectMessages } from '../../../../shared/components/select/select-messages.model';
import { SelectOption } from '../../../../shared/components/select/select-option.model';
import { ActionLabelsI18n, URLVerbs } from '../../../../shared/constants/app.constants';
import { CdForm } from '../../../../shared/forms/cd-form';
import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../../shared/forms/cd-validators';
import { FinishedTask } from '../../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../../shared/services/task-wrapper.service';

@Component({
  selector: 'cd-service-form',
  templateUrl: './service-form.component.html',
  styleUrls: ['./service-form.component.scss']
})
export class ServiceFormComponent extends CdForm implements OnInit {
  @ViewChild(NgbTypeahead, { static: false })
  typeahead: NgbTypeahead;

  serviceForm: CdFormGroup;
  action: string;
  resource: string;
  serviceTypes: string[] = [];
  hosts: any;
  labels: string[];
  labelClick = new Subject<string>();
  labelFocus = new Subject<string>();
  pools: Array<object>;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private cephServiceService: CephServiceService,
    private formBuilder: CdFormBuilder,
    private hostService: HostService,
    private poolService: PoolService,
    private router: Router,
    private taskWrapperService: TaskWrapperService
  ) {
    super();
    this.resource = $localize`service`;
    this.hosts = {
      options: [],
      messages: new SelectMessages({
        empty: $localize`There are no hosts.`,
        filter: $localize`Filter hosts`
      })
    };
    this.createForm();
  }

  createForm() {
    this.serviceForm = this.formBuilder.group({
      // Global
      service_type: [null, [Validators.required]],
      service_id: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'mds'
          }),
          CdValidators.requiredIf({
            service_type: 'nfs'
          }),
          CdValidators.requiredIf({
            service_type: 'iscsi'
          }),
          CdValidators.composeIf(
            {
              service_type: 'rgw'
            },
            [
              Validators.required,
              CdValidators.custom('rgwPattern', (value: string) => {
                if (_.isEmpty(value)) {
                  return false;
                }
                return !/^[^.]+\.[^.]+(\.[^.]+)?$/.test(value);
              })
            ]
          )
        ]
      ],
      placement: ['hosts'],
      label: [
        null,
        [
          CdValidators.requiredIf({
            placement: 'label',
            unmanaged: false
          })
        ]
      ],
      hosts: [[]],
      count: [null, [CdValidators.number(false), Validators.min(1)]],
      unmanaged: [false],
      // NFS & iSCSI
      pool: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'nfs',
            unmanaged: false
          }),
          CdValidators.requiredIf({
            service_type: 'iscsi',
            unmanaged: false
          })
        ]
      ],
      // NFS
      namespace: [null],
      // RGW
      rgw_frontend_port: [
        null,
        [CdValidators.number(false), Validators.min(1), Validators.max(65535)]
      ],
      // iSCSI
      trusted_ip_list: [null],
      api_port: [null, [CdValidators.number(false), Validators.min(1), Validators.max(65535)]],
      api_user: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'iscsi',
            unmanaged: false
          })
        ]
      ],
      api_password: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'iscsi',
            unmanaged: false
          })
        ]
      ],
      // RGW & iSCSI
      ssl: [false],
      ssl_cert: [
        '',
        [
          CdValidators.composeIf(
            {
              service_type: 'rgw',
              unmanaged: false,
              ssl: true
            },
            [Validators.required, CdValidators.sslCert()]
          ),
          CdValidators.composeIf(
            {
              service_type: 'iscsi',
              unmanaged: false,
              ssl: true
            },
            [Validators.required, CdValidators.sslCert()]
          )
        ]
      ],
      ssl_key: [
        '',
        [
          CdValidators.composeIf(
            {
              service_type: 'rgw',
              unmanaged: false,
              ssl: true
            },
            [Validators.required, CdValidators.sslPrivKey()]
          ),
          CdValidators.composeIf(
            {
              service_type: 'iscsi',
              unmanaged: false,
              ssl: true
            },
            [Validators.required, CdValidators.sslPrivKey()]
          )
        ]
      ]
    });
  }

  ngOnInit(): void {
    this.action = this.actionLabels.CREATE;
    this.cephServiceService.getKnownTypes().subscribe((resp: Array<string>) => {
      // Remove service type 'osd', this is deployed a different way.
      this.serviceTypes = _.difference(resp, ['osd']).sort();
    });
    this.hostService.list().subscribe((resp: object[]) => {
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
    this.poolService.getList().subscribe((resp: Array<object>) => {
      this.pools = resp;
    });
  }

  goToListView() {
    this.router.navigate(['/services']);
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

  fileUpload(files: FileList, controlName: string) {
    const file: File = files[0];
    const reader = new FileReader();
    reader.addEventListener('load', (event: ProgressEvent<FileReader>) => {
      const control: AbstractControl = this.serviceForm.get(controlName);
      control.setValue(event.target.result);
      control.markAsDirty();
      control.markAsTouched();
      control.updateValueAndValidity();
    });
    reader.readAsText(file, 'utf8');
  }

  onSubmit() {
    const self = this;
    const values: object = this.serviceForm.value;
    const serviceId: string = values['service_id'];
    const serviceType: string = values['service_type'];
    const serviceSpec: object = {
      service_type: serviceType,
      placement: {},
      unmanaged: values['unmanaged']
    };
    let serviceName: string = serviceType;
    if (_.isString(serviceId) && !_.isEmpty(serviceId)) {
      serviceName = `${serviceType}.${serviceId}`;
      serviceSpec['service_id'] = serviceId;
    }
    if (!values['unmanaged']) {
      switch (values['placement']) {
        case 'hosts':
          if (values['hosts'].length > 0) {
            serviceSpec['placement']['hosts'] = values['hosts'];
          }
          break;
        case 'label':
          serviceSpec['placement']['label'] = values['label'];
          break;
      }
      if (_.isNumber(values['count']) && values['count'] > 0) {
        serviceSpec['placement']['count'] = values['count'];
      }
      switch (serviceType) {
        case 'nfs':
          serviceSpec['pool'] = values['pool'];
          if (_.isString(values['namespace']) && !_.isEmpty(values['namespace'])) {
            serviceSpec['namespace'] = values['namespace'];
          }
          break;
        case 'rgw':
          if (_.isNumber(values['rgw_frontend_port']) && values['rgw_frontend_port'] > 0) {
            serviceSpec['rgw_frontend_port'] = values['rgw_frontend_port'];
          }
          serviceSpec['ssl'] = values['ssl'];
          if (values['ssl']) {
            serviceSpec['rgw_frontend_ssl_certificate'] = values['ssl_cert'].trim();
            serviceSpec['rgw_frontend_ssl_key'] = values['ssl_key'].trim();
          }
          break;
        case 'iscsi':
          serviceSpec['pool'] = values['pool'];
          if (_.isString(values['trusted_ip_list']) && !_.isEmpty(values['trusted_ip_list'])) {
            let parts = _.split(values['trusted_ip_list'], ',');
            parts = _.map(parts, _.trim);
            serviceSpec['trusted_ip_list'] = parts;
          }
          if (_.isNumber(values['api_port']) && values['api_port'] > 0) {
            serviceSpec['api_port'] = values['api_port'];
          }
          serviceSpec['api_user'] = values['api_user'];
          serviceSpec['api_password'] = values['api_password'];
          serviceSpec['api_secure'] = values['ssl'];
          if (values['ssl']) {
            serviceSpec['ssl_cert'] = values['ssl_cert'].trim();
            serviceSpec['ssl_key'] = values['ssl_key'].trim();
          }
          break;
      }
    }
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(`service/${URLVerbs.CREATE}`, {
          service_name: serviceName
        }),
        call: this.cephServiceService.create(serviceSpec)
      })
      .subscribe({
        error() {
          self.serviceForm.setErrors({ cdSubmitButton: true });
        },
        complete() {
          self.goToListView();
        }
      });
  }
}
