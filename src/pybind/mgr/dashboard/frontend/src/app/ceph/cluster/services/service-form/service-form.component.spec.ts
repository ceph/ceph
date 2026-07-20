import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { of } from 'rxjs';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { PaginateObservable } from '~/app/shared/api/paginate.model';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FormHelper, Mocks } from '~/testing/unit-test-helper';
import { ServiceFormComponent } from './service-form.component';
import { PoolService } from '~/app/shared/api/pool.service';
import { TextLabelListComponent } from '~/app/shared/components/text-label-list/text-label-list.component';
import { USER } from '~/app/shared/constants/app.constants';
import {
  CheckboxModule,
  InputModule,
  ModalModule,
  NumberModule,
  RadioModule,
  SelectModule
} from 'carbon-components-angular';

// for 'nvmeof' service
const mockPools = [
  Mocks.getPool('pool-1', 1, ['cephfs']),
  Mocks.getPool('rbd', 2),
  Mocks.getPool('pool-2', 3)
];
class MockPoolService {
  getList() {
    return of(mockPools);
  }
}

describe('ServiceFormComponent', () => {
  let component: ServiceFormComponent;
  let fixture: ComponentFixture<ServiceFormComponent>;
  let cephServiceService: CephServiceService;
  let form: CdFormGroup;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [ServiceFormComponent],
    providers: [NgbActiveModal, { provide: PoolService, useClass: MockPoolService }],
    imports: [
      HttpClientTestingModule,
      NgbTypeaheadModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      InputModule,
      SelectModule,
      NumberModule,
      ModalModule,
      CheckboxModule,
      RadioModule,
      TextLabelListComponent
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ServiceFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    form = component.serviceForm;
    formHelper = new FormHelper(form);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('should test form', () => {
    beforeEach(() => {
      cephServiceService = TestBed.inject(CephServiceService);
      spyOn(cephServiceService, 'create').and.stub();
    });

    it('should test placement (host)', () => {
      formHelper.setValue('service_type', 'crash');
      formHelper.setValue('placement', 'hosts');
      formHelper.setValue('hosts', [
        { content: 'mgr0', selected: true },
        { content: 'mon0', selected: true },
        { content: 'osd0', selected: true }
      ]);
      formHelper.setValue('count', 2);
      component.onSubmit();
      expect(cephServiceService.create).toHaveBeenCalledWith({
        service_type: 'crash',
        placement: {
          hosts: ['mgr0', 'mon0', 'osd0'],
          count: 2
        },
        unmanaged: false
      });
    });

    it('should test placement (label) with single select value', () => {
      // placement labels take only single value
      formHelper.setValue('service_type', 'mgr');
      formHelper.setValue('placement', 'label');
      formHelper.setValue('label', { content: 'foo', selected: true });

      component.onSubmit();

      expect(cephServiceService.create).toHaveBeenCalledWith({
        service_type: 'mgr',
        placement: {
          label: 'foo'
        },
        unmanaged: false
      });
    });

    it('should submit valid count', () => {
      formHelper.setValue('count', 1);
      component.onSubmit();
      formHelper.expectValid('count');
    });

    it('should submit invalid count (1)', () => {
      formHelper.setValue('count', 0);
      component.onSubmit();
      formHelper.expectError('count', 'min');
    });

    it('should submit invalid count (2)', () => {
      formHelper.setValue('count', 'abc');
      component.onSubmit();
      formHelper.expectError('count', 'pattern');
    });

    it('should test unmanaged', () => {
      formHelper.setValue('service_type', 'mgr');
      formHelper.setValue('service_id', 'svc');
      formHelper.setValue('placement', 'label');
      formHelper.setValue('label', 'bar');
      formHelper.setValue('unmanaged', true);
      component.onSubmit();
      expect(cephServiceService.create).toHaveBeenCalledWith({
        service_type: 'mgr',
        service_id: 'svc',
        placement: {},
        unmanaged: true
      });
    });

    it('should test various services', () => {
      _.forEach(
        ['alertmanager', 'crash', 'mds', 'mgr', 'mon', 'node-exporter', 'prometheus', 'rbd-mirror'],
        (serviceType) => {
          formHelper.setValue('service_type', serviceType);
          component.onSubmit();
          expect(cephServiceService.create).toHaveBeenCalledWith({
            service_type: serviceType,
            placement: {},
            unmanaged: false
          });
        }
      );
    });

    describe('should test service grafana', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'grafana');
      });

      it('should sumbit grafana', () => {
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'grafana',
          placement: {},
          unmanaged: false,
          initial_admin_password: null,
          port: null
        });
      });

      it('should sumbit grafana with custom port and initial password', () => {
        formHelper.setValue('grafana_port', 1234);
        formHelper.setValue('grafana_admin_password', 'foo');
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'grafana',
          placement: {},
          unmanaged: false,
          initial_admin_password: 'foo',
          port: 1234
        });
      });
    });

    describe('should test service nfs', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'nfs');
      });

      it('should submit nfs', () => {
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'nfs',
          placement: {},
          unmanaged: false
        });
      });
    });

    describe('should test service rgw', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'rgw');
        formHelper.setValue('service_id', 'svc');
      });

      it('should test rgw valid service id', () => {
        formHelper.setValue('service_id', 'svc.realm.zone');
        formHelper.expectValid('service_id');
        formHelper.setValue('service_id', 'svc');
        formHelper.expectValid('service_id');
      });

      it('should submit rgw with realm, zonegroup and zone', () => {
        formHelper.setValue('service_id', 'svc');
        formHelper.setValue('realm_name', 'my-realm');
        formHelper.setValue('zone_name', 'my-zone');
        formHelper.setValue('zonegroup_name', 'my-zonegroup');
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'rgw',
          service_id: 'svc',
          rgw_realm: 'my-realm',
          rgw_zone: 'my-zone',
          rgw_zonegroup: 'my-zonegroup',
          placement: {},
          unmanaged: false,
          ssl: false
        });
      });

      it('should submit rgw with port and ssl enabled', () => {
        formHelper.setValue('rgw_frontend_port', 1234);
        formHelper.setValue('ssl', true);
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'rgw',
          service_id: 'svc',
          rgw_realm: null,
          rgw_zone: null,
          rgw_zonegroup: null,
          placement: {},
          unmanaged: false,
          rgw_frontend_port: 1234,
          ssl: true,
          certificate_source: 'cephadm-signed'
        });
      });

      it('should submit rgw with virtual-host style bucket access and SSL', () => {
        formHelper.setValue('virtual_host_enabled', true);
        formHelper.setValue('ssl', true);
        formHelper.setValue('zonegroup_hostnames', ['s3.cephlab.com']);
        formHelper.setValue('wildcard_enabled', true);
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'rgw',
          service_id: 'svc',
          rgw_realm: null,
          rgw_zone: null,
          rgw_zonegroup: null,
          placement: {},
          unmanaged: false,
          ssl: true,
          certificate_source: 'cephadm-signed',
          zonegroup_hostnames: ['s3.cephlab.com'],
          wildcard_enabled: true
        });
      });

      it('should submit rgw with SSL and without virtual-host style bucket access', () => {
        formHelper.setValue('ssl', true);
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'rgw',
          service_id: 'svc',
          rgw_realm: null,
          rgw_zone: null,
          rgw_zonegroup: null,
          placement: {},
          unmanaged: false,
          ssl: true,
          certificate_source: 'cephadm-signed'
        });
      });

      it('should submit rgw with virtual-host style bucket access and SSL without wildcard certificate', () => {
        formHelper.setValue('virtual_host_enabled', true);
        formHelper.setValue('ssl', true);
        formHelper.setValue('zonegroup_hostnames', ['s3.cephlab.com']);
        formHelper.setValue('wildcard_enabled', false);
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'rgw',
          service_id: 'svc',
          rgw_realm: null,
          rgw_zone: null,
          rgw_zonegroup: null,
          placement: {},
          unmanaged: false,
          ssl: true,
          certificate_source: 'cephadm-signed',
          zonegroup_hostnames: ['s3.cephlab.com'],
          wildcard_enabled: false
        });
      });

      it('should submit valid rgw port (1)', () => {
        formHelper.setValue('rgw_frontend_port', 1);
        component.onSubmit();
        formHelper.expectValid('rgw_frontend_port');
      });

      it('should submit valid rgw port (2)', () => {
        formHelper.setValue('rgw_frontend_port', 65535);
        component.onSubmit();
        formHelper.expectValid('rgw_frontend_port');
      });

      it('should submit invalid rgw port (1)', () => {
        formHelper.setValue('rgw_frontend_port', 0);
        fixture.detectChanges();
        formHelper.expectError('rgw_frontend_port', 'min');
      });

      it('should submit invalid rgw port (2)', () => {
        formHelper.setValue('rgw_frontend_port', 65536);
        fixture.detectChanges();
        formHelper.expectError('rgw_frontend_port', 'max');
      });

      it('should submit invalid rgw port (3)', () => {
        formHelper.setValue('rgw_frontend_port', 'abc');
        component.onSubmit();
        formHelper.expectError('rgw_frontend_port', 'pattern');
      });

      it('should submit rgw w/o port', () => {
        formHelper.setValue('ssl', false);
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'rgw',
          service_id: 'svc',
          rgw_realm: null,
          rgw_zone: null,
          rgw_zonegroup: null,
          placement: {},
          unmanaged: false,
          ssl: false
        });
      });

      it('should not show private key field', () => {
        formHelper.setValue('ssl', true);
        fixture.detectChanges();
        const ssl_key = fixture.debugElement.query(By.css('#ssl_key'));
        expect(ssl_key).toBeNull();
      });

      it('should submit rgw with QAT Compression (None)', () => {
        formHelper.setValue('service_type', 'rgw');
        formHelper.setValue(component.serviceForm.get('qat').get('compression'), 'none');
      });

      it('should submit rgw with QAT Compression (Hardware)', () => {
        formHelper.setValue('service_type', 'rgw');
        formHelper.setValue(component.serviceForm.get('qat')?.get('compression'), 'hw');
      });

      it('should submit rgw with QAT Compression (Software)', () => {
        formHelper.setValue('service_type', 'rgw');
        formHelper.setValue(component.serviceForm.get('qat')?.get('compression'), 'sw');
      });

      it('should test .pem file', () => {
        const pemCert = `
-----BEGIN CERTIFICATE-----
iJ5IbgzlKPssdYwuAEI3yPZxX/g5vKBrgcyD3LttLL/DlElq/1xCnwVrv7WROSNu
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
mn/S7BNBEC7AGe5ajmN+8hBTGdACUXe8rwMNrtTy/MwBZ0VpJsAAjJh+aptZh5yB
-----END CERTIFICATE-----
-----BEGIN RSA PRIVATE KEY-----
x4Ea7kGVgx9kWh5XjWz9wjZvY49UKIT5ppIAWPMbLl3UpfckiuNhTA==
-----END RSA PRIVATE KEY-----`;
        formHelper.setValue('ssl', true);
        formHelper.setValue('ssl_cert', pemCert);
        fixture.detectChanges();
        formHelper.expectValid('ssl_cert');
      });
    });

    describe('should test service iscsi', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'iscsi');
        formHelper.setValue('pool', 'xyz');
        formHelper.setValue('api_user', USER);
        formHelper.setValue('api_password', 'password');
        formHelper.setValue('ssl', false);
      });

      it('should submit iscsi', () => {
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'iscsi',
          placement: {},
          unmanaged: false,
          pool: 'xyz',
          api_user: USER,
          api_password: 'password',
          api_secure: false
        });
      });

      it('should submit iscsi with trusted ips', () => {
        formHelper.setValue('ssl', true);
        formHelper.setValue('trusted_ip_list', [' 172.16.0.5', '192.1.1.10  ']);
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'iscsi',
          placement: {},
          unmanaged: false,
          pool: 'xyz',
          api_user: USER,
          api_password: 'password',
          api_secure: true,
          certificate_source: 'cephadm-signed',
          trusted_ip_list: ['172.16.0.5', '192.1.1.10']
        });
      });

      it('should submit iscsi with port', () => {
        formHelper.setValue('api_port', 456);
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'iscsi',
          placement: {},
          unmanaged: false,
          pool: 'xyz',
          api_user: USER,
          api_password: 'password',
          api_secure: false,
          api_port: 456
        });
      });

      it('should submit valid iscsi port (1)', () => {
        formHelper.setValue('api_port', 1);
        component.onSubmit();
        formHelper.expectValid('api_port');
      });

      it('should submit valid iscsi port (2)', () => {
        formHelper.setValue('api_port', 65535);
        component.onSubmit();
        formHelper.expectValid('api_port');
      });

      it('should submit invalid iscsi port (1)', () => {
        formHelper.setValue('api_port', 0);
        component.onSubmit();
        formHelper.expectError('api_port', 'min');
      });

      it('should submit invalid iscsi port (2)', () => {
        formHelper.setValue('api_port', 65536);
        component.onSubmit();
        formHelper.expectError('api_port', 'max');
      });

      it('should submit invalid iscsi port (3)', () => {
        formHelper.setValue('api_port', 'abc');
        component.onSubmit();
        formHelper.expectError('api_port', 'pattern');
      });

      it('should throw error when there is no pool', () => {
        formHelper.expectErrorChange('pool', '', 'required');
      });
    });

    describe('should test service nvmeof', () => {
      beforeEach(() => {
        component.serviceType = 'nvmeof';
        formHelper.setValue('service_type', 'nvmeof');
        component.ngOnInit();
        fixture.detectChanges();
      });

      it('should set rbd pools correctly onInit', () => {
        expect(component.pools.length).toBe(3);
        expect(component.rbdPools.length).toBe(2);
      });

      it('should set default values correctly onInit', () => {
        expect(form.get('service_type').value).toBe('nvmeof');
        expect(form.get('group').value).toBe('default');
        expect(form.get('service_id').value).toBe('default');
      });

      it('should reflect correct values on group change', () => {
        expect(component.serviceForm.get('group')?.value).toBe('default');
        const groupInput = fixture.debugElement.query(By.css('#group')).nativeElement;
        groupInput.value = 'foo';
        groupInput.dispatchEvent(new Event('input'));
        groupInput.dispatchEvent(new Event('change'));
        fixture.detectChanges();
        expect(form.get('group').value).toBe('foo');
        expect(form.get('service_id').value).toBe('foo');
      });

      it('should hide pool selector in create mode', () => {
        const poolEl = fixture.debugElement.query(By.css('#pool'));
        expect(poolEl).toBeNull();
      });

      it('should throw error when there is no service id', () => {
        formHelper.expectErrorChange('service_id', '', 'required');
      });

      it('should throw error when there is no group', () => {
        formHelper.expectErrorChange('group', '', 'required');
      });

      it('should hide the count element when service_type is "nvmeof"', () => {
        const countEl = fixture.debugElement.query(By.css('#count'));
        expect(countEl).toBeNull();
      });

      it('should not show certs and keys field with mTLS disabled', () => {
        formHelper.setValue('ssl', true);
        fixture.detectChanges();
        const root_ca_cert = fixture.debugElement.query(By.css('#root_ca_cert'));
        const client_cert = fixture.debugElement.query(By.css('#client_cert'));
        const client_key = fixture.debugElement.query(By.css('#client_key'));
        const server_cert = fixture.debugElement.query(By.css('#server_cert'));
        const server_key = fixture.debugElement.query(By.css('#server_key'));
        expect(root_ca_cert).toBeNull();
        expect(client_cert).toBeNull();
        expect(client_key).toBeNull();
        expect(server_cert).toBeNull();
        expect(server_key).toBeNull();
      });

      it('should not show certs and keys field with internal mTLS', () => {
        formHelper.setValue('enable_mtls', true);
        formHelper.setValue('certificateType', 'internal');
        fixture.detectChanges();
        const root_ca_cert = fixture.debugElement.query(By.css('#root_ca_cert'));
        const client_cert = fixture.debugElement.query(By.css('#client_cert'));
        const client_key = fixture.debugElement.query(By.css('#client_key'));
        const server_cert = fixture.debugElement.query(By.css('#server_cert'));
        const server_key = fixture.debugElement.query(By.css('#server_key'));
        expect(root_ca_cert).toBeNull();
        expect(client_cert).toBeNull();
        expect(client_key).toBeNull();
        expect(server_cert).toBeNull();
        expect(server_key).toBeNull();
      });

      it('should submit nvmeof without mTLS', () => {
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'nvmeof',
          service_id: 'default',
          placement: {},
          unmanaged: false,
          group: 'default',
          enable_auth: false
        });
      });

      it('should submit nvmeof with mTLS', () => {
        formHelper.setValue('enable_mtls', true);
        formHelper.setValue('certificateType', 'external');
        formHelper.setValue('root_ca_cert', 'root_ca_cert');
        formHelper.setValue('client_cert', 'client_cert');
        formHelper.setValue('client_key', 'client_key');
        formHelper.setValue('server_cert', 'server_cert');
        formHelper.setValue('server_key', 'server_key');
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'nvmeof',
          service_id: 'rbd.default',
          placement: {},
          unmanaged: false,
          group: 'default',
          enable_auth: true,
          ssl: true,
          pool: 'rbd',
          certificate_source: 'inline',
          root_ca_cert: 'root_ca_cert',
          client_cert: 'client_cert',
          client_key: 'client_key',
          server_cert: 'server_cert',
          server_key: 'server_key'
        });
      });

      it('should submit nvmeof with internal mTLS', () => {
        formHelper.setValue('enable_mtls', true);
        formHelper.setValue('certificateType', 'internal');
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'nvmeof',
          service_id: 'default',
          placement: {},
          unmanaged: false,
          group: 'default',
          enable_auth: true,
          ssl: true,
          certificate_source: 'cephadm-signed'
        });
      });
    });

    describe('should test service smb', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'smb');
        formHelper.setValue('service_id', 'foo');
        formHelper.setValue('cluster_id', 'cluster_foo');
        formHelper.setValue('config_uri', 'rados://.smb/foo/scc.toml');
        formHelper.setValue('custom_dns', [' 192.168.76.204', '192.168.76.205 ']);
      });

      it('should submit smb', () => {
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'smb',
          placement: {},
          unmanaged: false,
          service_id: 'foo',
          cluster_id: 'cluster_foo',
          config_uri: 'rados://.smb/foo/scc.toml',
          custom_dns: ['192.168.76.204', '192.168.76.205']
        });
      });
    });

    describe('should test service ingress', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'ingress');
        formHelper.setValue('backend_service', 'rgw.foo');
        formHelper.setValue('virtual_ip', '192.168.20.1/24');
        formHelper.setValue('ssl', false);
      });

      it('should submit ingress', () => {
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'ingress',
          placement: {},
          unmanaged: false,
          backend_service: 'rgw.foo',
          service_id: 'rgw.foo',
          virtual_ip: '192.168.20.1/24',
          ssl: false
        });
      });

      it('should pre-populate the service id', () => {
        component.prePopulateId();
        const prePopulatedID = component.serviceForm.getValue('service_id');
        expect(prePopulatedID).toBe('rgw.foo');
      });

      it('should submit valid frontend and monitor port', () => {
        // min value
        formHelper.setValue('frontend_port', 1);
        formHelper.setValue('monitor_port', 1);
        component.onSubmit();
        formHelper.expectValid('frontend_port');
        formHelper.expectValid('monitor_port');

        // max value
        formHelper.setValue('frontend_port', 65535);
        formHelper.setValue('monitor_port', 65535);
        component.onSubmit();
        formHelper.expectValid('frontend_port');
        formHelper.expectValid('monitor_port');
      });

      it('should submit invalid frontend and monitor port', () => {
        // min
        formHelper.setValue('frontend_port', 0);
        formHelper.setValue('monitor_port', 0);
        component.onSubmit();
        formHelper.expectError('frontend_port', 'min');
        formHelper.expectError('monitor_port', 'min');

        // max
        formHelper.setValue('frontend_port', 65536);
        formHelper.setValue('monitor_port', 65536);
        component.onSubmit();
        formHelper.expectError('frontend_port', 'max');
        formHelper.expectError('monitor_port', 'max');

        // pattern
        formHelper.setValue('frontend_port', 'abc');
        formHelper.setValue('monitor_port', 'abc');
        component.onSubmit();
        formHelper.expectError('frontend_port', 'pattern');
        formHelper.expectError('monitor_port', 'pattern');
      });

      it('should not show private key field with ssl enabled', () => {
        formHelper.setValue('ssl', true);
        component.onSubmit();
        const ssl_key = fixture.debugElement.query(By.css('#ssl_key'));
        expect(ssl_key).toBeNull();
      });

      it('should test .pem file with ssl enabled', () => {
        const pemCert = `
-----BEGIN CERTIFICATE-----
iJ5IbgzlKPssdYwuAEI3yPZxX/g5vKBrgcyD3LttLL/DlElq/1xCnwVrv7WROSNu
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
mn/S7BNBEC7AGe5ajmN+8hBTGdACUXe8rwMNrtTy/MwBZ0VpJsAAjJh+aptZh5yB
-----END CERTIFICATE-----
-----BEGIN RSA PRIVATE KEY-----
x4Ea7kGVgx9kWh5XjWz9wjZvY49UKIT5ppIAWPMbLl3UpfckiuNhTA==
-----END RSA PRIVATE KEY-----`;
        formHelper.setValue('ssl', true);
        formHelper.setValue('ssl_cert', pemCert);
        component.onSubmit();
        formHelper.expectValid('ssl_cert');
      });
    });

    describe('should test service snmp-gateway', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'snmp-gateway');
        formHelper.setValue('snmp_destination', '192.168.20.1:8443');
      });

      it('should test snmp-gateway service with V2c', () => {
        formHelper.setValue('snmp_version', 'V2c');
        formHelper.setValue('snmp_community', 'public');
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'snmp-gateway',
          placement: {},
          unmanaged: false,
          snmp_version: 'V2c',
          snmp_destination: '192.168.20.1:8443',
          credentials: {
            snmp_community: 'public'
          }
        });
      });

      it('should test snmp-gateway service with V3', () => {
        formHelper.setValue('snmp_version', 'V3');
        formHelper.setValue('engine_id', '800C53F00000');
        formHelper.setValue('auth_protocol', 'SHA');
        formHelper.setValue('privacy_protocol', 'DES');
        formHelper.setValue('snmp_v3_auth_username', 'testuser');
        formHelper.setValue('snmp_v3_auth_password', 'testpass');
        formHelper.setValue('snmp_v3_priv_password', 'testencrypt');
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'snmp-gateway',
          placement: {},
          unmanaged: false,
          snmp_version: 'V3',
          snmp_destination: '192.168.20.1:8443',
          engine_id: '800C53F00000',
          auth_protocol: 'SHA',
          privacy_protocol: 'DES',
          credentials: {
            snmp_v3_auth_username: 'testuser',
            snmp_v3_auth_password: 'testpass',
            snmp_v3_priv_password: 'testencrypt'
          }
        });
      });

      it('should submit invalid snmp destination', () => {
        formHelper.setValue('snmp_version', 'V2c');
        formHelper.setValue('snmp_destination', '192.168.20.1');
        formHelper.setValue('snmp_community', 'public');
        formHelper.expectError('snmp_destination', 'snmpDestinationPattern');
      });

      it('should submit invalid snmp engine id', () => {
        formHelper.setValue('snmp_version', 'V3');
        formHelper.setValue('snmp_destination', '192.168.20.1');
        formHelper.setValue('engine_id', 'AABBCCDDE');
        formHelper.setValue('auth_protocol', 'SHA');
        formHelper.setValue('privacy_protocol', 'DES');
        formHelper.setValue('snmp_v3_auth_username', 'testuser');
        formHelper.setValue('snmp_v3_auth_password', 'testpass');

        formHelper.expectError('engine_id', 'snmpEngineIdPattern');
      });
    });

    describe('should test service mds', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'mds');
        const paginate_obs = new PaginateObservable<any>(of({}));
        spyOn(cephServiceService, 'list').and.returnValue(paginate_obs);
      });

      it('should test mds valid service id', () => {
        formHelper.setValue('service_id', 'svc123');
        formHelper.expectValid('service_id');
        formHelper.setValue('service_id', 'svc_id-1');
        formHelper.expectValid('service_id');
      });

      it('should test mds invalid service id', () => {
        formHelper.setValue('service_id', '123');
        formHelper.expectError('service_id', 'mdsPattern');
        formHelper.setValue('service_id', '123svc');
        formHelper.expectError('service_id', 'mdsPattern');
        formHelper.setValue('service_id', 'svc#1');
        formHelper.expectError('service_id', 'mdsPattern');
      });
    });

    describe('check edit fields', () => {
      beforeEach(() => {
        component.editing = true;
      });

      it('should check whether edit field is correctly loaded', () => {
        const paginate_obs = new PaginateObservable<any>(of({}));
        const cephServiceSpy = spyOn(cephServiceService, 'list').and.returnValue(paginate_obs);
        component.ngOnInit();
        expect(cephServiceSpy).toBeCalledTimes(2);
        expect(component.action).toBe('Edit');
        const serviceType = fixture.componentInstance.serviceForm.get('service_type');
        const serviceId = fixture.componentInstance.serviceForm.get('service_id');
        expect(serviceType.disabled).toBeTruthy();
        expect(serviceId.disabled).toBeTruthy();
      });

      it('should not edit groups for nvmeof service', () => {
        component.serviceType = 'nvmeof';
        formHelper.setValue('service_type', 'nvmeof');
        component.ngOnInit();
        fixture.detectChanges();
        const groupId = fixture.debugElement.query(By.css('#group')).nativeElement;
        expect(groupId.disabled).toBeTruthy();
      });

      it('should update nvmeof service to disable mTLS', () => {
        spyOn(cephServiceService, 'update').and.stub();
        component.serviceType = 'nvmeof';
        formHelper.setValue('service_type', 'nvmeof');
        formHelper.setValue('group', 'default');
        formHelper.setValue('enable_mtls', false);
        component.onSubmit();
        expect(cephServiceService.update).toHaveBeenCalledWith({
          service_type: 'nvmeof',
          placement: {},
          unmanaged: false,
          group: 'default',
          enable_auth: false
        });
      });
    });
  });

  // ---------------------------------------------------------------------------
  // RGW multisite filtering and subscription behaviour
  // ---------------------------------------------------------------------------
  describe('RGW multisite filtering', () => {
    // Shared multisite fixture data
    const realmA = { id: 'r1', name: 'realm-a' };
    const realmB = { id: 'r2', name: 'realm-b' };
    const zgA1 = { id: 'zg1', name: 'zg-a1', realm_id: 'r1', zones: [{ name: 'zone-a1' }] };
    const zgA2 = { id: 'zg2', name: 'zg-a2', realm_id: 'r1', zones: [{ name: 'zone-a2' }] };
    const zgB1 = { id: 'zg3', name: 'zg-b1', realm_id: 'r2', zones: [{ name: 'zone-b1' }] };
    const zoneA1 = { id: 'z1', name: 'zone-a1' };
    const zoneA2 = { id: 'z2', name: 'zone-a2' };
    const zoneB1 = { id: 'z3', name: 'zone-b1' };

    const realmsInfo = {
      realms: [realmA, realmB],
      default_realm: 'r1'
    };
    const zonegroupsInfo = {
      zonegroups: [zgA1, zgA2, zgB1],
      default_zonegroup: 'zg1'
    };
    const zonesInfo = {
      zones: [zoneA1, zoneA2, zoneB1],
      default_zone: 'z1'
    };

    let rgwRealmService: RgwRealmService;
    let rgwZonegroupService: RgwZonegroupService;
    let rgwZoneService: RgwZoneService;
    let rgwMultisiteService: RgwMultisiteService;

    beforeEach(() => {
      rgwRealmService = TestBed.inject(RgwRealmService);
      rgwZonegroupService = TestBed.inject(RgwZonegroupService);
      rgwZoneService = TestBed.inject(RgwZoneService);
      rgwMultisiteService = TestBed.inject(RgwMultisiteService);

      spyOn(rgwRealmService, 'getAllRealmsInfo').and.returnValue(of(realmsInfo));
      spyOn(rgwZonegroupService, 'getAllZonegroupsInfo').and.returnValue(of(zonegroupsInfo));
      spyOn(rgwZoneService, 'getAllZonesInfo').and.returnValue(of(zonesInfo));
      spyOn(rgwMultisiteService, 'getRgwModuleStatus').and.returnValue(of(false));
    });

    describe('create mode – initial filtering', () => {
      beforeEach(fakeAsync(() => {
        formHelper.setValue('service_type', 'rgw');
        component.setRgwFields();
        tick();
      }));

      it('should populate filteredZonegroupList with all zonegroups when no realm is selected', () => {
        // Default realm-a is selected; its two zonegroups should appear.
        expect(component.filteredZonegroupList.length).toBe(2);
        expect(component.filteredZonegroupList.map((zg) => zg.name)).toEqual(
          jasmine.arrayContaining(['zg-a1', 'zg-a2'])
        );
      });

      it('should populate filteredZoneList based on the default zonegroup', () => {
        // Default zonegroup is zg-a1 which has zone-a1.
        expect(component.filteredZoneList.length).toBe(1);
        expect(component.filteredZoneList[0].name).toBe('zone-a1');
      });
    });

    describe('realm change cascades zonegroup and zone lists', () => {
      beforeEach(fakeAsync(() => {
        formHelper.setValue('service_type', 'rgw');
        component.setRgwFields();
        tick();
      }));

      it('switching realm filters zonegroups to the new realm', fakeAsync(() => {
        form.get('realm_name').setValue('realm-b');
        tick();

        expect(component.filteredZonegroupList.length).toBe(1);
        expect(component.filteredZonegroupList[0].name).toBe('zg-b1');
      }));

      it('switching realm auto-cascades zonegroup then zone for the new realm', fakeAsync(() => {
        form.get('realm_name').setValue('realm-b');
        tick();

        // The subscription chain (realm → zonegroup → zone) resolves within the same
        // tick: realm-b's only zonegroup (zg-b1) is auto-selected, then its first zone
        // (zone-b1) is auto-selected.  zone_name should be non-null and point to
        // the first zone of the new realm's first zonegroup.
        expect(form.get('zone_name').value).toBe('zone-b1');
      }));

      it('switching realm then zonegroup populates zones for the new zonegroup', fakeAsync(() => {
        form.get('realm_name').setValue('realm-b');
        tick();

        // The first zonegroup of realm-b (zg-b1) is auto-selected by the subscription.
        tick();
        expect(component.filteredZoneList.length).toBe(1);
        expect(component.filteredZoneList[0].name).toBe('zone-b1');
      }));
    });

    describe('zonegroup change cascades zone list', () => {
      beforeEach(fakeAsync(() => {
        formHelper.setValue('service_type', 'rgw');
        component.setRgwFields();
        tick();
      }));

      it('switching zonegroup within the same realm updates filteredZoneList', fakeAsync(() => {
        form.get('zonegroup_name').setValue('zg-a2');
        tick();

        expect(component.filteredZoneList.length).toBe(1);
        expect(component.filteredZoneList[0].name).toBe('zone-a2');
      }));

      it('auto-selects the first zone of the new zonegroup', fakeAsync(() => {
        form.get('zonegroup_name').setValue('zg-a2');
        tick();

        expect(form.get('zone_name').value).toBe('zone-a2');
      }));
    });

    describe('edit mode – initial population from spec', () => {
      beforeEach(fakeAsync(() => {
        component.editing = true;
        component.setRgwFields('realm-a', 'zg-a2', 'zone-a2');
        tick();
      }));

      it('should set filteredZonegroupList to realm-a zonegroups', () => {
        expect(component.filteredZonegroupList.map((zg) => zg.name)).toEqual(
          jasmine.arrayContaining(['zg-a1', 'zg-a2'])
        );
      });

      it('should set filteredZoneList based on the spec zonegroup', () => {
        expect(component.filteredZoneList.length).toBe(1);
        expect(component.filteredZoneList[0].name).toBe('zone-a2');
      });

      it('should not show the realm-changed banner immediately after loading', () => {
        expect(component.showRgwRealmChangedInfo).toBe(false);
      });
    });

    describe('edit mode – realm-changed banner (showRgwRealmChangedInfo)', () => {
      beforeEach(fakeAsync(() => {
        component.editing = true;
        component.setRgwFields('realm-a', 'zg-a1', 'zone-a1');
        tick();
        // Banner must be false right after initial load.
        expect(component.showRgwRealmChangedInfo).toBe(false);
      }));

      it('shows the banner when the realm is changed', fakeAsync(() => {
        form.get('realm_name').setValue('realm-b');
        tick();
        form.get('zonegroup_name').setValue('zg-b1');
        tick();
        form.get('zone_name').setValue('zone-b1');
        tick();

        expect(component.showRgwRealmChangedInfo).toBe(true);
      }));

      it('shows the banner when only the zonegroup is changed', fakeAsync(() => {
        form.get('zonegroup_name').setValue('zg-a2');
        tick();
        form.get('zone_name').setValue('zone-a2');
        tick();

        expect(component.showRgwRealmChangedInfo).toBe(true);
      }));

      it('shows the banner when only the zone is changed', fakeAsync(() => {
        // Add a second zone to zg-a1 so there is something to switch to
        component.filteredZoneList = [
          { id: 'z1', name: 'zone-a1' } as any,
          { id: 'z4', name: 'zone-a1-extra' } as any
        ];
        form.get('zone_name').setValue('zone-a1-extra');
        tick();

        expect(component.showRgwRealmChangedInfo).toBe(true);
      }));

      it('hides the banner when reverted back to original values', fakeAsync(() => {
        // Change then revert
        form.get('zonegroup_name').setValue('zg-a2');
        tick();
        form.get('zonegroup_name').setValue('zg-a1');
        tick();
        form.get('zone_name').setValue('zone-a1');
        tick();

        expect(component.showRgwRealmChangedInfo).toBe(false);
      }));
    });

    describe('no-realm scenario – filteredZoneList falls back to all zones', () => {
      const noRealmZonegroupsInfo = {
        zonegroups: [{ id: 'zg0', name: 'default', realm_id: null, zones: [] }],
        default_zonegroup: 'zg0'
      };
      const noRealmZonesInfo = {
        zones: [{ id: 'z0', name: 'default' }],
        default_zone: 'z0'
      };

      beforeEach(fakeAsync(() => {
        (rgwZonegroupService.getAllZonegroupsInfo as jasmine.Spy).and.returnValue(
          of(noRealmZonegroupsInfo)
        );
        (rgwZoneService.getAllZonesInfo as jasmine.Spy).and.returnValue(of(noRealmZonesInfo));
        (rgwRealmService.getAllRealmsInfo as jasmine.Spy).and.returnValue(
          of({ realms: [], default_realm: null })
        );

        component.setRgwFields();
        tick();
      }));

      it('should show realm creation form when no realms exist', () => {
        expect(component.showRealmCreationForm).toBe(true);
      });

      it('should fall back to all zones when the zonegroup has no zones list', fakeAsync(() => {
        form.get('zonegroup_name').setValue('default');
        tick();

        expect(component.filteredZoneList.length).toBeGreaterThan(0);
      }));
    });
  });
});
