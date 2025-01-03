import { HttpParams } from '@angular/common/http';
import { Component, Input, OnInit, ViewChild } from '@angular/core';
import { AbstractControl, UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { NgbActiveModal, NgbModalRef, NgbTypeahead } from '@ng-bootstrap/ng-bootstrap';
import { ListItem } from 'carbon-components-angular';
import _ from 'lodash';
import { forkJoin, merge, Observable, Subject, Subscription } from 'rxjs';
import { debounceTime, distinctUntilChanged, filter, map } from 'rxjs/operators';
import { Pool } from '~/app/ceph/pool/pool';
import { CreateRgwServiceEntitiesComponent } from '~/app/ceph/rgw/create-rgw-service-entities/create-rgw-service-entities.component';
import { RgwRealm, RgwZonegroup, RgwZone } from '~/app/ceph/rgw/models/rgw-multisite';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { HostService } from '~/app/shared/api/host.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import {
  ActionLabelsI18n,
  TimerServiceInterval,
  URLVerbs,
  SSL_PROTOCOLS,
  SSL_CIPHERS
} from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { TimerService } from '~/app/shared/services/timer.service';

@Component({
  selector: 'cd-service-form',
  templateUrl: './service-form.component.html',
  styleUrls: ['./service-form.component.scss']
})
export class ServiceFormComponent extends CdForm implements OnInit {
  public sub = new Subscription();

  readonly MDS_SVC_ID_PATTERN = /^[a-zA-Z_.-][a-zA-Z0-9_.-]*$/;
  readonly SNMP_DESTINATION_PATTERN = /^[^\:]+:[0-9]/;
  readonly SNMP_ENGINE_ID_PATTERN = /^[0-9A-Fa-f]{10,64}/g;
  readonly INGRESS_SUPPORTED_SERVICE_TYPES = ['rgw', 'nfs'];
  readonly SMB_CONFIG_URI_PATTERN = /^(http:|https:|rados:|rados:mon-config-key:)/;
  readonly OAUTH2_ISSUER_URL_PATTERN = /^(https?:\/\/)?([a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+)(:[0-9]{1,5})?(\/.*)?$/;
  readonly SSL_CIPHERS_PATTERN = /^[a-zA-Z0-9\-:]+$/;
  readonly DEFAULT_SSL_PROTOCOL_ITEM = [{ content: 'TLSv1.3', selected: true }];
  @ViewChild(NgbTypeahead, { static: false })
  typeahead: NgbTypeahead;

  @Input() hiddenServices: string[] = [];

  @Input() editing = false;

  @Input() serviceName: string;

  @Input() serviceType: string;

  serviceForm: CdFormGroup;
  action: string;
  resource: string;
  serviceTypes: string[] = [];
  serviceIds: string[] = [];
  hosts: any;
  labels: string[];
  labelClick = new Subject<string>();
  labelFocus = new Subject<string>();
  pools: Array<Pool>;
  rbdPools: Array<Pool>;
  services: Array<CephServiceSpec> = [];
  pageURL: string;
  serviceList: CephServiceSpec[];
  multisiteInfo: object[] = [];
  defaultRealmId = '';
  defaultZonegroupId = '';
  defaultZoneId = '';
  realmList: RgwRealm[] = [];
  zonegroupList: RgwZonegroup[] = [];
  zoneList: RgwZone[] = [];
  bsModalRef: NgbModalRef;
  defaultZonegroup: RgwZonegroup;
  showRealmCreationForm = false;
  defaultsInfo: { defaultRealmName: string; defaultZonegroupName: string; defaultZoneName: string };
  realmNames: string[];
  zonegroupNames: string[];
  zoneNames: string[];
  smbFeaturesList = ['domain'];
  currentURL: string;
  port: number = 443;
  sslProtocolsItems: Array<ListItem> = Object.values(SSL_PROTOCOLS).map((protocol) => ({
    content: protocol,
    selected: true
  }));
  sslCiphersItems: Array<ListItem> = Object.values(SSL_CIPHERS).map((cipher) => ({
    content: cipher,
    selected: false
  }));
  showMgmtGatewayMessage: boolean = false;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private cephServiceService: CephServiceService,
    private formBuilder: CdFormBuilder,
    private hostService: HostService,
    private poolService: PoolService,
    private rbdService: RbdService,
    private router: Router,
    private taskWrapperService: TaskWrapperService,
    public timerService: TimerService,
    public timerServiceVariable: TimerServiceInterval,
    public rgwRealmService: RgwRealmService,
    public rgwZonegroupService: RgwZonegroupService,
    public rgwZoneService: RgwZoneService,
    public rgwMultisiteService: RgwMultisiteService,
    private route: ActivatedRoute,
    public activeModal: NgbActiveModal,
    public modalService: ModalService
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
          CdValidators.composeIf(
            {
              service_type: 'mds'
            },
            [
              Validators.required,
              CdValidators.custom('mdsPattern', (value: string) => {
                if (_.isEmpty(value)) {
                  return false;
                }
                return !this.MDS_SVC_ID_PATTERN.test(value);
              })
            ]
          ),
          CdValidators.requiredIf({
            service_type: 'nfs'
          }),
          CdValidators.requiredIf({
            service_type: 'iscsi'
          }),
          CdValidators.requiredIf({
            service_type: 'nvmeof'
          }),
          CdValidators.requiredIf({
            service_type: 'ingress'
          }),
          CdValidators.requiredIf({
            service_type: 'smb'
          }),
          CdValidators.composeIf(
            {
              service_type: 'rgw'
            },
            [Validators.required]
          ),
          CdValidators.custom('uniqueName', (service_id: string) => {
            return this.serviceIds && this.serviceIds.includes(service_id);
          })
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
      count: [null, [CdValidators.number(false)]],
      unmanaged: [false],
      // iSCSI
      // NVMe/TCP
      pool: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'iscsi'
          }),
          CdValidators.requiredIf({
            service_type: 'nvmeof'
          })
        ]
      ],
      group: [
        'default',
        CdValidators.requiredIf({
          service_type: 'nvmeof'
        })
      ],
      enable_mtls: [false],
      root_ca_cert: [
        null,
        [
          CdValidators.composeIf(
            {
              service_type: 'nvmeof',
              enable_mtls: true
            },
            [Validators.required]
          )
        ]
      ],
      client_cert: [
        null,
        [
          CdValidators.composeIf(
            {
              service_type: 'nvmeof',
              enable_mtls: true
            },
            [Validators.required]
          )
        ]
      ],
      client_key: [
        null,
        [
          CdValidators.composeIf(
            {
              service_type: 'nvmeof',
              enable_mtls: true
            },
            [Validators.required]
          )
        ]
      ],
      server_cert: [
        null,
        [
          CdValidators.composeIf(
            {
              service_type: 'nvmeof',
              enable_mtls: true
            },
            [Validators.required]
          )
        ]
      ],
      server_key: [
        null,
        [
          CdValidators.composeIf(
            {
              service_type: 'nvmeof',
              enable_mtls: true
            },
            [Validators.required]
          )
        ]
      ],
      // RGW
      rgw_frontend_port: [null, [CdValidators.number(false)]],
      realm_name: [null],
      zonegroup_name: [null],
      zone_name: [null],
      // iSCSI
      trusted_ip_list: [null],
      api_port: [null, [CdValidators.number(false)]],
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
      // smb
      cluster_id: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'smb'
          })
        ]
      ],
      features: new CdFormGroup(
        this.smbFeaturesList.reduce((acc: object, e) => {
          acc[e] = new UntypedFormControl(false);
          return acc;
        }, {})
      ),
      config_uri: [
        null,
        [
          CdValidators.composeIf(
            {
              service_type: 'smb'
            },
            [
              Validators.required,
              CdValidators.custom('configUriPattern', (value: string) => {
                if (_.isEmpty(value)) {
                  return false;
                }
                return !this.SMB_CONFIG_URI_PATTERN.test(value);
              })
            ]
          )
        ]
      ],
      custom_dns: [null],
      join_sources: [null],
      user_sources: [null],
      include_ceph_users: [null],
      // Ingress
      backend_service: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'ingress'
          })
        ]
      ],
      virtual_ip: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'ingress'
          })
        ]
      ],
      frontend_port: [
        null,
        [
          CdValidators.number(false),
          CdValidators.requiredIf({
            service_type: 'ingress'
          })
        ]
      ],
      monitor_port: [
        null,
        [
          CdValidators.number(false),
          CdValidators.requiredIf({
            service_type: 'ingress'
          })
        ]
      ],
      virtual_interface_networks: [null],
      ssl_protocols: [this.DEFAULT_SSL_PROTOCOL_ITEM],
      ssl_ciphers: [
        null,
        [
          CdValidators.custom('invalidPattern', (ciphers: string) => {
            if (_.isEmpty(ciphers)) {
              return false;
            }
            return !this.SSL_CIPHERS_PATTERN.test(ciphers);
          })
        ]
      ],
      // RGW, Ingress & iSCSI
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
            [Validators.required, CdValidators.pemCert()]
          ),
          CdValidators.composeIf(
            {
              service_type: 'iscsi',
              unmanaged: false,
              ssl: true
            },
            [Validators.required, CdValidators.sslCert()]
          ),
          CdValidators.composeIf(
            {
              service_type: 'ingress',
              unmanaged: false,
              ssl: true
            },
            [Validators.required, CdValidators.pemCert()]
          ),
          CdValidators.composeIf(
            {
              service_type: 'oauth2-proxy',
              unmanaged: false,
              ssl: true
            },
            [Validators.required, CdValidators.sslCert()]
          ),
          CdValidators.composeIf(
            {
              service_type: 'mgmt-gateway',
              unmanaged: false,
              ssl: false
            },
            [CdValidators.sslCert()]
          )
        ]
      ],
      ssl_key: [
        '',
        [
          CdValidators.composeIf(
            {
              service_type: 'iscsi',
              unmanaged: false,
              ssl: true
            },
            [Validators.required, CdValidators.sslPrivKey()]
          ),
          CdValidators.composeIf(
            {
              service_type: 'oauth2-proxy',
              unmanaged: false,
              ssl: true
            },
            [Validators.required, CdValidators.sslPrivKey()]
          ),
          CdValidators.composeIf(
            {
              service_type: 'mgmt-gateway',
              unmanaged: false,
              ssl: false
            },
            [CdValidators.sslPrivKey()]
          )
        ]
      ],
      // mgmt-gateway
      enable_auth: [null],
      port: [443, [CdValidators.number(false)]],
      // snmp-gateway
      snmp_version: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'snmp-gateway'
          })
        ]
      ],
      snmp_destination: [
        null,
        {
          validators: [
            CdValidators.requiredIf({
              service_type: 'snmp-gateway'
            }),
            CdValidators.custom('snmpDestinationPattern', (value: string) => {
              if (_.isEmpty(value)) {
                return false;
              }
              return !this.SNMP_DESTINATION_PATTERN.test(value);
            })
          ]
        }
      ],
      engine_id: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'snmp-gateway'
          }),
          CdValidators.custom('snmpEngineIdPattern', (value: string) => {
            if (_.isEmpty(value)) {
              return false;
            }
            return !this.SNMP_ENGINE_ID_PATTERN.test(value);
          })
        ]
      ],
      auth_protocol: [
        'SHA',
        [
          CdValidators.requiredIf({
            service_type: 'snmp-gateway'
          })
        ]
      ],
      privacy_protocol: [null],
      snmp_community: [
        null,
        [
          CdValidators.requiredIf({
            snmp_version: 'V2c'
          })
        ]
      ],
      snmp_v3_auth_username: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'snmp-gateway'
          })
        ]
      ],
      snmp_v3_auth_password: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'snmp-gateway'
          })
        ]
      ],
      snmp_v3_priv_password: [
        null,
        [
          CdValidators.requiredIf({
            privacy_protocol: { op: '!empty' }
          })
        ]
      ],
      grafana_port: [null, [CdValidators.number(false)]],
      grafana_admin_password: [null],
      // oauth2-proxy
      provider_display_name: [
        'My OIDC provider',
        [
          CdValidators.requiredIf({
            service_type: 'oauth2-proxy'
          })
        ]
      ],
      client_id: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'oauth2-proxy'
          })
        ]
      ],
      client_secret: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'oauth2-proxy'
          })
        ]
      ],
      oidc_issuer_url: [
        null,
        [
          CdValidators.requiredIf({
            service_type: 'oauth2-proxy'
          }),
          CdValidators.custom('validUrl', (url: string) => {
            if (_.isEmpty(url)) {
              return false;
            }
            return !this.OAUTH2_ISSUER_URL_PATTERN.test(url);
          })
        ]
      ],
      https_address: [null, [CdValidators.oauthAddressTest()]],
      redirect_url: [null],
      allowlist_domains: [null]
    });
  }

  resolveRoute() {
    if (this.router.url.includes('services/(modal:create')) {
      this.pageURL = 'services';
      this.route.params.subscribe((params: { type: string }) => {
        if (params?.type) {
          this.serviceType = params.type;
          this.serviceForm.get('service_type').setValue(this.serviceType);
        }
      });
    } else if (this.router.url.includes('services/(modal:edit')) {
      this.editing = true;
      this.pageURL = 'services';
      this.route.params.subscribe((params: { type: string; name: string }) => {
        this.serviceName = params.name;
        this.serviceType = params.type;
      });
    }
  }

  ngOnInit(): void {
    this.action = this.actionLabels.CREATE;
    this.resolveRoute();

    this.cephServiceService
      .list(new HttpParams({ fromObject: { limit: -1, offset: 0 } }))
      .observable.subscribe((services: CephServiceSpec[]) => {
        this.serviceList = services;
        this.services = services.filter((service: any) =>
          this.INGRESS_SUPPORTED_SERVICE_TYPES.includes(service.service_type)
        );
      });

    this.cephServiceService.getKnownTypes().subscribe((resp: Array<string>) => {
      // Remove service types:
      // osd       - This is deployed a different way.
      // container - This should only be used in the CLI.
      this.hiddenServices.push('osd', 'container');

      this.serviceTypes = _.difference(resp, this.hiddenServices).sort();
    });
    this.hostService.getAllHosts().subscribe((resp: object[]) => {
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
    this.poolService.getList().subscribe((resp: Pool[]) => {
      this.pools = resp;
      this.rbdPools = this.pools.filter(this.rbdService.isRBDPool);
      if (!this.editing && this.serviceType) {
        this.onServiceTypeChange(this.serviceType);
      }
    });

    if (this.editing) {
      this.action = this.actionLabels.EDIT;
      this.disableForEditing(this.serviceType);
      this.cephServiceService
        .list(new HttpParams({ fromObject: { limit: -1, offset: 0 } }), this.serviceName)
        .observable.subscribe((response: CephServiceSpec[]) => {
          const formKeys = ['service_type', 'service_id', 'unmanaged'];
          formKeys.forEach((keys) => {
            this.serviceForm.get(keys).setValue(response[0][keys]);
          });
          if (!response[0]['unmanaged']) {
            const placementKey = Object.keys(response[0]['placement'])[0];
            let placementValue: string;
            ['hosts', 'label'].indexOf(placementKey) >= 0
              ? (placementValue = placementKey)
              : (placementValue = 'hosts');
            this.serviceForm.get('placement').setValue(placementValue);
            this.serviceForm.get('count').setValue(response[0]['placement']['count']);
            if (response[0]?.placement[placementValue]) {
              this.serviceForm.get(placementValue).setValue(response[0]?.placement[placementValue]);
            }
          }
          switch (this.serviceType) {
            case 'iscsi':
              const specKeys = ['pool', 'api_password', 'api_user', 'trusted_ip_list', 'api_port'];
              specKeys.forEach((key) => {
                this.serviceForm.get(key).setValue(response[0].spec[key]);
              });
              this.serviceForm.get('ssl').setValue(response[0].spec?.api_secure);
              if (response[0].spec?.api_secure) {
                this.serviceForm.get('ssl_cert').setValue(response[0].spec?.ssl_cert);
                this.serviceForm.get('ssl_key').setValue(response[0].spec?.ssl_key);
              }
              break;
            case 'nvmeof':
              this.serviceForm.get('pool').setValue(response[0].spec.pool);
              this.serviceForm.get('group').setValue(response[0].spec.group);
              this.serviceForm.get('enable_mtls').setValue(response[0].spec?.enable_auth);
              this.serviceForm.get('root_ca_cert').setValue(response[0].spec?.root_ca_cert);
              this.serviceForm.get('client_cert').setValue(response[0].spec?.client_cert);
              this.serviceForm.get('client_key').setValue(response[0].spec?.client_key);
              this.serviceForm.get('server_cert').setValue(response[0].spec?.server_cert);
              this.serviceForm.get('server_key').setValue(response[0].spec?.server_key);
              break;
            case 'rgw':
              this.serviceForm
                .get('rgw_frontend_port')
                .setValue(response[0].spec?.rgw_frontend_port);
              this.setRgwFields(
                response[0].spec?.rgw_realm,
                response[0].spec?.rgw_zonegroup,
                response[0].spec?.rgw_zone
              );
              this.serviceForm.get('ssl').setValue(response[0].spec?.ssl);
              if (response[0].spec?.ssl) {
                this.serviceForm
                  .get('ssl_cert')
                  .setValue(response[0].spec?.rgw_frontend_ssl_certificate);
              }
              break;
            case 'ingress':
              const ingressSpecKeys = [
                'backend_service',
                'virtual_ip',
                'frontend_port',
                'monitor_port',
                'virtual_interface_networks',
                'ssl'
              ];
              ingressSpecKeys.forEach((key) => {
                this.serviceForm.get(key).setValue(response[0].spec[key]);
              });
              if (response[0].spec?.ssl) {
                this.serviceForm.get('ssl_cert').setValue(response[0].spec?.ssl_cert);
                this.serviceForm.get('ssl_key').setValue(response[0].spec?.ssl_key);
              }
              break;
            case 'mgmt-gateway':
              let hrefSplitted = window.location.href.split(':');
              this.currentURL = hrefSplitted[0] + hrefSplitted[1];
              this.port = response[0].spec?.port;

              if (response[0].spec?.ssl_protocols) {
                let selectedValues: Array<ListItem> = [];
                for (const value of response[0].spec.ssl_protocols) {
                  selectedValues.push({ content: value, selected: true });
                }
                this.serviceForm.get('ssl_protocols').setValue(selectedValues);
              }
              if (response[0].spec?.ssl_ciphers) {
                this.serviceForm
                  .get('ssl_ciphers')
                  .setValue(response[0].spec?.ssl_ciphers.join(':'));
              }
              if (response[0].spec?.ssl_cert) {
                this.serviceForm.get('ssl_cert').setValue(response[0].spec.ssl_certificate);
              }
              if (response[0].spec?.ssl_key) {
                this.serviceForm.get('ssl_key').setValue(response[0].spec.ssl_certificate_key);
              }
              if (response[0].spec?.enable_auth) {
                this.serviceForm.get('enable_auth').setValue(response[0].spec.enable_auth);
              }
              if (response[0].spec?.port) {
                this.serviceForm.get('port').setValue(response[0].spec.port);
              }
              break;
            case 'smb':
              const smbSpecKeys = [
                'cluster_id',
                'config_uri',
                'features',
                'join_sources',
                'user_sources',
                'custom_dns',
                'include_ceph_users'
              ];
              smbSpecKeys.forEach((key) => {
                if (key === 'features') {
                  if (response[0].spec?.features) {
                    response[0].spec.features.forEach((feature) => {
                      this.serviceForm.get(`features.${feature}`).setValue(true);
                    });
                  }
                } else {
                  this.serviceForm.get(key).setValue(response[0].spec[key]);
                }
              });
              break;
            case 'snmp-gateway':
              const snmpCommonSpecKeys = ['snmp_version', 'snmp_destination'];
              snmpCommonSpecKeys.forEach((key) => {
                this.serviceForm.get(key).setValue(response[0].spec[key]);
              });
              if (this.serviceForm.getValue('snmp_version') === 'V3') {
                const snmpV3SpecKeys = [
                  'engine_id',
                  'auth_protocol',
                  'privacy_protocol',
                  'snmp_v3_auth_username',
                  'snmp_v3_auth_password',
                  'snmp_v3_priv_password'
                ];
                snmpV3SpecKeys.forEach((key) => {
                  if (key !== null) {
                    if (
                      key === 'snmp_v3_auth_username' ||
                      key === 'snmp_v3_auth_password' ||
                      key === 'snmp_v3_priv_password'
                    ) {
                      this.serviceForm.get(key).setValue(response[0].spec['credentials'][key]);
                    } else {
                      this.serviceForm.get(key).setValue(response[0].spec[key]);
                    }
                  }
                });
              } else {
                this.serviceForm
                  .get('snmp_community')
                  .setValue(response[0].spec['credentials']['snmp_community']);
              }
              break;
            case 'grafana':
              this.serviceForm.get('grafana_port').setValue(response[0].spec.port);
              this.serviceForm
                .get('grafana_admin_password')
                .setValue(response[0].spec.initial_admin_password);
              break;
            case 'oauth2-proxy':
              const oauth2SpecKeys = [
                'https_address',
                'provider_display_name',
                'client_id',
                'client_secret',
                'oidc_issuer_url',
                'redirect_url',
                'allowlist_domains'
              ];
              oauth2SpecKeys.forEach((key) => {
                this.serviceForm.get(key).setValue(response[0].spec[key]);
              });
              if (response[0].spec?.ssl) {
                this.serviceForm.get('ssl_cert').setValue(response[0].spec?.ssl_cert);
                this.serviceForm.get('ssl_key').setValue(response[0].spec?.ssl_key);
              }
          }
        });
    }
    this.detectChanges();
  }

  detectChanges(): void {
    const service_type = this.serviceForm.get('service_type');
    if (service_type) {
      service_type.valueChanges.subscribe((value) => {
        if (value === 'mgmt-gateway') {
          const port = this.serviceForm.get('port');
          if (port) {
            port.valueChanges.subscribe((_) => {
              this.showMgmtGatewayMessage = true;
            });
          }
          const ssl_protocols = this.serviceForm.get('ssl_protocols');
          if (ssl_protocols) {
            ssl_protocols.valueChanges.subscribe((_) => {
              this.showMgmtGatewayMessage = true;
            });
          }
          const ssl_ciphers = this.serviceForm.get('ssl_ciphers');
          if (ssl_ciphers) {
            ssl_ciphers.valueChanges.subscribe((_) => {
              this.showMgmtGatewayMessage = true;
            });
          }
        }
      });
    }
  }

  getDefaultsEntitiesForRgw(
    defaultRealmId: string,
    defaultZonegroupId: string,
    defaultZoneId: string
  ): { defaultRealmName: string; defaultZonegroupName: string; defaultZoneName: string } {
    const defaultRealm = this.realmList.find((x: { id: string }) => x.id === defaultRealmId);
    const defaultZonegroup = this.zonegroupList.find(
      (x: { id: string }) => x.id === defaultZonegroupId
    );
    const defaultZone = this.zoneList.find((x: { id: string }) => x.id === defaultZoneId);
    const defaultRealmName = defaultRealm !== undefined ? defaultRealm.name : null;
    const defaultZonegroupName = defaultZonegroup !== undefined ? defaultZonegroup.name : 'default';
    const defaultZoneName = defaultZone !== undefined ? defaultZone.name : 'default';
    if (defaultZonegroupName === 'default' && !this.zonegroupNames.includes(defaultZonegroupName)) {
      const defaultZonegroup = new RgwZonegroup();
      defaultZonegroup.name = 'default';
      this.zonegroupList.push(defaultZonegroup);
    }
    if (defaultZoneName === 'default' && !this.zoneNames.includes(defaultZoneName)) {
      const defaultZone = new RgwZone();
      defaultZone.name = 'default';
      this.zoneList.push(defaultZone);
    }
    return {
      defaultRealmName: defaultRealmName,
      defaultZonegroupName: defaultZonegroupName,
      defaultZoneName: defaultZoneName
    };
  }

  getDefaultPlacementCount(serviceType: string) {
    /**
     * `defaults` from src/pybind/mgr/cephadm/module.py
     */
    switch (serviceType) {
      case 'mon':
        this.serviceForm.get('count').setValue(5);
        break;
      case 'mgr':
      case 'mds':
      case 'rgw':
      case 'ingress':
      case 'rbd-mirror':
        this.serviceForm.get('count').setValue(2);
        break;
      case 'iscsi':
      case 'cephfs-mirror':
      case 'nfs':
      case 'grafana':
      case 'alertmanager':
      case 'prometheus':
      case 'loki':
      case 'container':
      case 'snmp-gateway':
      case 'elastic-serach':
      case 'jaeger-collector':
      case 'jaeger-query':
      case 'smb':
      case 'oauth2-proxy':
      case 'mgmt-gateway':
        this.serviceForm.get('count').setValue(1);
        break;
      default:
        this.serviceForm.get('count').setValue(null);
    }
  }

  setRgwFields(realm_name?: string, zonegroup_name?: string, zone_name?: string) {
    const observables = [
      this.rgwRealmService.getAllRealmsInfo(),
      this.rgwZonegroupService.getAllZonegroupsInfo(),
      this.rgwZoneService.getAllZonesInfo()
    ];
    this.sub = forkJoin(observables).subscribe(
      (multisiteInfo: [object, object, object]) => {
        this.multisiteInfo = multisiteInfo;
        this.realmList =
          this.multisiteInfo[0] !== undefined && this.multisiteInfo[0].hasOwnProperty('realms')
            ? this.multisiteInfo[0]['realms']
            : [];
        this.zonegroupList =
          this.multisiteInfo[1] !== undefined && this.multisiteInfo[1].hasOwnProperty('zonegroups')
            ? this.multisiteInfo[1]['zonegroups']
            : [];
        this.zoneList =
          this.multisiteInfo[2] !== undefined && this.multisiteInfo[2].hasOwnProperty('zones')
            ? this.multisiteInfo[2]['zones']
            : [];
        this.realmNames = this.realmList.map((realm) => {
          return realm['name'];
        });
        this.zonegroupNames = this.zonegroupList.map((zonegroup) => {
          return zonegroup['name'];
        });
        this.zoneNames = this.zoneList.map((zone) => {
          return zone['name'];
        });
        this.defaultRealmId = multisiteInfo[0]['default_realm'];
        this.defaultZonegroupId = multisiteInfo[1]['default_zonegroup'];
        this.defaultZoneId = multisiteInfo[2]['default_zone'];
        this.defaultsInfo = this.getDefaultsEntitiesForRgw(
          this.defaultRealmId,
          this.defaultZonegroupId,
          this.defaultZoneId
        );
        if (!this.editing) {
          this.serviceForm.get('realm_name').setValue(this.defaultsInfo['defaultRealmName']);
          this.serviceForm
            .get('zonegroup_name')
            .setValue(this.defaultsInfo['defaultZonegroupName']);
          this.serviceForm.get('zone_name').setValue(this.defaultsInfo['defaultZoneName']);
        } else {
          if (realm_name && !this.realmNames.includes(realm_name)) {
            const realm = new RgwRealm();
            realm.name = realm_name;
            this.realmList.push(realm);
          }
          if (zonegroup_name && !this.zonegroupNames.includes(zonegroup_name)) {
            const zonegroup = new RgwZonegroup();
            zonegroup.name = zonegroup_name;
            this.zonegroupList.push(zonegroup);
          }
          if (zone_name && !this.zoneNames.includes(zone_name)) {
            const zone = new RgwZone();
            zone.name = zone_name;
            this.zoneList.push(zone);
          }
          if (zonegroup_name === undefined && zone_name === undefined) {
            zonegroup_name = 'default';
            zone_name = 'default';
          }
          this.serviceForm.get('realm_name').setValue(realm_name);
          this.serviceForm.get('zonegroup_name').setValue(zonegroup_name);
          this.serviceForm.get('zone_name').setValue(zone_name);
        }
        if (this.realmList.length === 0) {
          this.showRealmCreationForm = true;
        } else {
          this.showRealmCreationForm = false;
        }
      },
      (_error) => {
        const defaultZone = new RgwZone();
        defaultZone.name = 'default';
        const defaultZonegroup = new RgwZonegroup();
        defaultZonegroup.name = 'default';
        this.zoneList.push(defaultZone);
        this.zonegroupList.push(defaultZonegroup);
      }
    );
  }

  setNvmeServiceId() {
    const pool = this.serviceForm.get('pool').value;
    const group = this.serviceForm.get('group').value;
    if (pool && group) {
      this.serviceForm.get('service_id').setValue(`${pool}.${group}`);
    } else if (pool) {
      this.serviceForm.get('service_id').setValue(pool);
    } else if (group) {
      this.serviceForm.get('service_id').setValue(group);
    } else {
      this.serviceForm.get('service_id').setValue(null);
    }
  }

  setNvmeDefaultPool() {
    const defaultPool =
      this.rbdPools?.find((p: Pool) => p.pool_name === 'rbd')?.pool_name ||
      this.rbdPools?.[0].pool_name;
    this.serviceForm.get('pool').setValue(defaultPool);
  }

  requiresServiceId(serviceType: string) {
    return ['mds', 'rgw', 'nfs', 'iscsi', 'nvmeof', 'smb', 'ingress'].includes(serviceType);
  }

  setServiceId(serviceId: string): void {
    const requiresServiceId: boolean = this.requiresServiceId(serviceId);
    if (requiresServiceId && serviceId === 'nvmeof') {
      this.setNvmeDefaultPool();
      this.setNvmeServiceId();
    } else if (requiresServiceId) {
      this.serviceForm.get('service_id').setValue(null);
    } else {
      this.serviceForm.get('service_id').setValue(serviceId);
    }
  }

  onServiceTypeChange(selectedServiceType: string) {
    this.setServiceId(selectedServiceType);

    this.serviceIds = this.serviceList
      ?.filter((service) => service['service_type'] === selectedServiceType)
      .map((service) => service['service_id']);

    this.getDefaultPlacementCount(selectedServiceType);

    if (selectedServiceType === 'rgw') {
      this.setRgwFields();
    }
    if (selectedServiceType === 'mgmt-gateway') {
      let hrefSplitted = window.location.href.split(':');
      this.currentURL = hrefSplitted[0] + hrefSplitted[1];
      // mgmt-gateway lacks HA for now
      this.serviceForm.get('count').disable();
    } else {
      this.serviceForm.get('count').enable();
    }
  }

  onPlacementChange(selected: string) {
    if (selected === 'label') {
      this.serviceForm.get('count').setValue(null);
    }
  }

  disableForEditing(serviceType: string) {
    const disableForEditKeys = ['service_type', 'service_id'];
    disableForEditKeys.forEach((key) => {
      this.serviceForm.get(key).disable();
    });
    switch (serviceType) {
      case 'ingress':
        this.serviceForm.get('backend_service').disable();
        break;
      case 'nvmeof':
        this.serviceForm.get('pool').disable();
        this.serviceForm.get('group').disable();
        break;
    }
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

  prePopulateId() {
    const control: AbstractControl = this.serviceForm.get('service_id');
    const backendService = this.serviceForm.getValue('backend_service');
    // Set Id as read-only
    control.reset({ value: backendService, disabled: true });
  }

  onSubmit() {
    const self = this;
    const values: object = this.serviceForm.getRawValue();
    const serviceType: string = values['service_type'];
    let taskUrl = `service/${URLVerbs.CREATE}`;
    if (this.editing) {
      taskUrl = `service/${URLVerbs.EDIT}`;
    }
    const serviceSpec: object = {
      service_type: serviceType,
      placement: {},
      unmanaged: values['unmanaged']
    };
    if (serviceType === 'rgw') {
      serviceSpec['rgw_realm'] = values['realm_name'] ? values['realm_name'] : null;
      serviceSpec['rgw_zonegroup'] =
        values['zonegroup_name'] !== 'default' ? values['zonegroup_name'] : null;
      serviceSpec['rgw_zone'] = values['zone_name'] !== 'default' ? values['zone_name'] : null;
    }

    const serviceId: string = values['service_id'];
    let serviceName: string = serviceType;
    if (_.isString(serviceId) && !_.isEmpty(serviceId) && serviceId !== serviceType) {
      serviceName = `${serviceType}.${serviceId}`;
      serviceSpec['service_id'] = serviceId;
    }

    // These services has some fields to be
    // filled out even if unmanaged is true
    switch (serviceType) {
      case 'ingress':
        serviceSpec['backend_service'] = values['backend_service'];
        serviceSpec['service_id'] = values['backend_service'];
        if (_.isNumber(values['frontend_port']) && values['frontend_port'] > 0) {
          serviceSpec['frontend_port'] = values['frontend_port'];
        }
        if (_.isString(values['virtual_ip']) && !_.isEmpty(values['virtual_ip'])) {
          serviceSpec['virtual_ip'] = values['virtual_ip'].trim();
        }
        if (_.isNumber(values['monitor_port']) && values['monitor_port'] > 0) {
          serviceSpec['monitor_port'] = values['monitor_port'];
        }
        break;

      case 'nvmeof':
        serviceSpec['pool'] = values['pool'];
        serviceSpec['group'] = values['group'];
        serviceSpec['enable_auth'] = values['enable_mtls'];
        if (values['enable_mtls']) {
          serviceSpec['root_ca_cert'] = values['root_ca_cert'];
          serviceSpec['client_cert'] = values['client_cert'];
          serviceSpec['client_key'] = values['client_key'];
          serviceSpec['server_cert'] = values['server_cert'];
          serviceSpec['server_key'] = values['server_key'];
        }
        break;
      case 'iscsi':
        serviceSpec['pool'] = values['pool'];
        break;

      case 'smb':
        serviceSpec['cluster_id'] = values['cluster_id']?.trim();
        serviceSpec['config_uri'] = values['config_uri']?.trim();
        for (const feature in values['features']) {
          if (values['features'][feature]) {
            (serviceSpec['features'] = serviceSpec['features'] || []).push(feature);
          }
        }
        serviceSpec['custom_dns'] = values['custom_dns']?.trim();
        serviceSpec['join_sources'] = values['join_sources']?.trim();
        serviceSpec['user_sources'] = values['user_sources']?.trim();
        serviceSpec['include_ceph_users'] = values['include_ceph_users']?.trim();
        break;

      case 'snmp-gateway':
        serviceSpec['credentials'] = {};
        serviceSpec['snmp_version'] = values['snmp_version'];
        serviceSpec['snmp_destination'] = values['snmp_destination'];
        if (values['snmp_version'] === 'V3') {
          serviceSpec['engine_id'] = values['engine_id'];
          serviceSpec['auth_protocol'] = values['auth_protocol'];
          serviceSpec['credentials']['snmp_v3_auth_username'] = values['snmp_v3_auth_username'];
          serviceSpec['credentials']['snmp_v3_auth_password'] = values['snmp_v3_auth_password'];
          if (values['privacy_protocol'] !== null) {
            serviceSpec['privacy_protocol'] = values['privacy_protocol'];
            serviceSpec['credentials']['snmp_v3_priv_password'] = values['snmp_v3_priv_password'];
          }
        } else {
          serviceSpec['credentials']['snmp_community'] = values['snmp_community'];
        }
        break;
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
        case 'rgw':
          if (_.isNumber(values['rgw_frontend_port']) && values['rgw_frontend_port'] > 0) {
            serviceSpec['rgw_frontend_port'] = values['rgw_frontend_port'];
          }
          serviceSpec['ssl'] = values['ssl'];
          if (values['ssl']) {
            serviceSpec['rgw_frontend_ssl_certificate'] = values['ssl_cert']?.trim();
          }
          break;
        case 'iscsi':
          if (_.isString(values['trusted_ip_list']) && !_.isEmpty(values['trusted_ip_list'])) {
            serviceSpec['trusted_ip_list'] = values['trusted_ip_list'].trim();
          }
          if (_.isNumber(values['api_port']) && values['api_port'] > 0) {
            serviceSpec['api_port'] = values['api_port'];
          }
          serviceSpec['api_user'] = values['api_user'];
          serviceSpec['api_password'] = values['api_password'];
          serviceSpec['api_secure'] = values['ssl'];
          if (values['ssl']) {
            serviceSpec['ssl_cert'] = values['ssl_cert']?.trim();
            serviceSpec['ssl_key'] = values['ssl_key']?.trim();
          }
          break;
        case 'ingress':
          serviceSpec['ssl'] = values['ssl'];
          if (values['ssl']) {
            serviceSpec['ssl_cert'] = values['ssl_cert']?.trim();
            serviceSpec['ssl_key'] = values['ssl_key']?.trim();
          }
          serviceSpec['virtual_interface_networks'] = values['virtual_interface_networks'];
          break;
        case 'mgmt-gateway':
          serviceSpec['ssl_certificate'] = values['ssl_cert']?.trim();
          serviceSpec['ssl_certificate_key'] = values['ssl_key']?.trim();
          serviceSpec['enable_auth'] = values['enable_auth'];
          serviceSpec['port'] = values['port'];
          if (serviceSpec['port'] === (443 || 80)) {
            // omit port default values due to issues with redirect_url on the backend
            delete serviceSpec['port'];
          }
          serviceSpec['ssl_protocols'] = [];
          if (values['ssl_protocols'] != this.DEFAULT_SSL_PROTOCOL_ITEM) {
            for (const key of Object.keys(values['ssl_protocols'])) {
              serviceSpec['ssl_protocols'].push(values['ssl_protocols'][key]['content']);
            }
          }
          serviceSpec['ssl_ciphers'] = values['ssl_ciphers']?.trim().split(':');
          break;
        case 'grafana':
          serviceSpec['port'] = values['grafana_port'];
          serviceSpec['initial_admin_password'] = values['grafana_admin_password'];
          break;
        case 'oauth2-proxy':
          serviceSpec['provider_display_name'] = values['provider_display_name']?.trim();
          serviceSpec['client_id'] = values['client_id']?.trim();
          serviceSpec['client_secret'] = values['client_secret']?.trim();
          serviceSpec['oidc_issuer_url'] = values['oidc_issuer_url']?.trim();
          serviceSpec['https_address'] = values['https_address']?.trim();
          serviceSpec['redirect_url'] = values['redirect_url']?.trim();
          serviceSpec['allowlist_domains'] = values['allowlist_domains']
            .split(',')
            .map((domain: string) => {
              return domain.trim();
            });
          if (values['ssl']) {
            serviceSpec['ssl_cert'] = values['ssl_cert']?.trim();
            serviceSpec['ssl_key'] = values['ssl_key']?.trim();
          }
          break;
      }
    }
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          service_name: serviceName
        }),
        call: this.editing
          ? this.cephServiceService.update(serviceSpec)
          : this.cephServiceService.create(serviceSpec)
      })
      .subscribe({
        error() {
          self.serviceForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.pageURL === 'services'
            ? this.router.navigate([this.pageURL, { outlets: { modal: null } }])
            : this.activeModal.close();
        }
      });
  }

  clearValidations() {
    const snmpVersion = this.serviceForm.getValue('snmp_version');
    const privacyProtocol = this.serviceForm.getValue('privacy_protocol');
    if (snmpVersion === 'V3') {
      this.serviceForm.get('snmp_community').clearValidators();
    } else {
      this.serviceForm.get('engine_id').clearValidators();
      this.serviceForm.get('auth_protocol').clearValidators();
      this.serviceForm.get('privacy_protocol').clearValidators();
      this.serviceForm.get('snmp_v3_auth_username').clearValidators();
      this.serviceForm.get('snmp_v3_auth_password').clearValidators();
    }
    if (privacyProtocol === null) {
      this.serviceForm.get('snmp_v3_priv_password').clearValidators();
    }
  }

  createMultisiteSetup() {
    this.bsModalRef = this.modalService.show(CreateRgwServiceEntitiesComponent, {
      size: 'lg'
    });
    this.bsModalRef.componentInstance.submitAction.subscribe(() => {
      this.setRgwFields();
    });
  }
}
