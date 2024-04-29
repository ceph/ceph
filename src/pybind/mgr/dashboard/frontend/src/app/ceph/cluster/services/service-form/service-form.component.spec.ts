import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { PaginateObservable } from '~/app/shared/api/paginate.model';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { ServiceFormComponent } from './service-form.component';

describe('ServiceFormComponent', () => {
  let component: ServiceFormComponent;
  let fixture: ComponentFixture<ServiceFormComponent>;
  let cephServiceService: CephServiceService;
  let form: CdFormGroup;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [ServiceFormComponent],
    providers: [NgbActiveModal],
    imports: [
      HttpClientTestingModule,
      NgbTypeaheadModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
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
      formHelper.setValue('hosts', ['mgr0', 'mon0', 'osd0']);
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

    it('should test placement (label)', () => {
      formHelper.setValue('service_type', 'mgr');
      formHelper.setValue('placement', 'label');
      formHelper.setValue('label', 'foo');
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
          rgw_frontend_ssl_certificate: '',
          ssl: true
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
        formHelper.setValue('api_user', 'user');
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
          api_user: 'user',
          api_password: 'password',
          api_secure: false
        });
      });

      it('should submit iscsi with trusted ips', () => {
        formHelper.setValue('ssl', true);
        formHelper.setValue('trusted_ip_list', ' 172.16.0.5, 192.1.1.10  ');
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'iscsi',
          placement: {},
          unmanaged: false,
          pool: 'xyz',
          api_user: 'user',
          api_password: 'password',
          api_secure: true,
          ssl_cert: '',
          ssl_key: '',
          trusted_ip_list: '172.16.0.5, 192.1.1.10'
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
          api_user: 'user',
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
        fixture.detectChanges();
        formHelper.expectError('api_port', 'min');
      });

      it('should submit invalid iscsi port (2)', () => {
        formHelper.setValue('api_port', 65536);
        fixture.detectChanges();
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

    describe('should test service smb', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'smb');
        formHelper.setValue('service_id', 'foo');
        formHelper.setValue('cluster_id', 'cluster_foo');
        formHelper.setValue('config_uri', 'rados://.smb/foo/scc.toml');
      });

      it('should submit smb', () => {
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'smb',
          placement: {},
          unmanaged: false,
          service_id: 'foo',
          cluster_id: 'cluster_foo',
          config_uri: 'rados://.smb/foo/scc.toml'
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
          virtual_interface_networks: null,
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
        fixture.detectChanges();
        formHelper.expectValid('frontend_port');
        formHelper.expectValid('monitor_port');

        // max value
        formHelper.setValue('frontend_port', 65535);
        formHelper.setValue('monitor_port', 65535);
        fixture.detectChanges();
        formHelper.expectValid('frontend_port');
        formHelper.expectValid('monitor_port');
      });

      it('should submit invalid frontend and monitor port', () => {
        // min
        formHelper.setValue('frontend_port', 0);
        formHelper.setValue('monitor_port', 0);
        fixture.detectChanges();
        formHelper.expectError('frontend_port', 'min');
        formHelper.expectError('monitor_port', 'min');

        // max
        formHelper.setValue('frontend_port', 65536);
        formHelper.setValue('monitor_port', 65536);
        fixture.detectChanges();
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
        fixture.detectChanges();
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
        fixture.detectChanges();
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
        const serviceType = fixture.debugElement.query(By.css('#service_type')).nativeElement;
        const serviceId = fixture.debugElement.query(By.css('#service_id')).nativeElement;
        expect(serviceType.disabled).toBeTruthy();
        expect(serviceId.disabled).toBeTruthy();
      });
    });
  });
});
