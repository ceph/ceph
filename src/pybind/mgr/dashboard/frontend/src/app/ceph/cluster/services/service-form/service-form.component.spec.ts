import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, FormHelper } from '../../../../../testing/unit-test-helper';
import { CephServiceService } from '../../../../shared/api/ceph-service.service';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { SharedModule } from '../../../../shared/shared.module';
import { ServiceFormComponent } from './service-form.component';

describe('ServiceFormComponent', () => {
  let component: ServiceFormComponent;
  let fixture: ComponentFixture<ServiceFormComponent>;
  let cephServiceService: CephServiceService;
  let form: CdFormGroup;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [ServiceFormComponent],
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
      formHelper.setValue('service_type', 'rgw');
      formHelper.setValue('placement', 'label');
      formHelper.setValue('label', 'bar');
      formHelper.setValue('rgw_frontend_port', 4567);
      formHelper.setValue('unmanaged', true);
      component.onSubmit();
      expect(cephServiceService.create).toHaveBeenCalledWith({
        service_type: 'rgw',
        placement: {},
        unmanaged: true
      });
    });

    it('should test various services', () => {
      _.forEach(
        [
          'alertmanager',
          'crash',
          'grafana',
          'mds',
          'mgr',
          'mon',
          'node-exporter',
          'prometheus',
          'rbd-mirror'
        ],
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

    describe('should test service nfs', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'nfs');
        formHelper.setValue('pool', 'foo');
      });

      it('should submit nfs with namespace', () => {
        formHelper.setValue('namespace', 'bar');
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'nfs',
          placement: {},
          unmanaged: false,
          pool: 'foo',
          namespace: 'bar'
        });
      });

      it('should submit nfs w/o namespace', () => {
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'nfs',
          placement: {},
          unmanaged: false,
          pool: 'foo'
        });
      });
    });

    describe('should test service rgw', () => {
      beforeEach(() => {
        formHelper.setValue('service_type', 'rgw');
      });

      it('should test rgw valid service id', () => {
        formHelper.setValue('service_id', 'foo.bar');
        formHelper.expectValid('service_id');
        formHelper.setValue('service_id', 'foo.bar.bas');
        formHelper.expectValid('service_id');
      });

      it('should test rgw invalid service id', () => {
        formHelper.setValue('service_id', 'foo');
        formHelper.expectError('service_id', 'rgwPattern');
        formHelper.setValue('service_id', 'foo.');
        formHelper.expectError('service_id', 'rgwPattern');
        formHelper.setValue('service_id', 'foo.bar.');
        formHelper.expectError('service_id', 'rgwPattern');
        formHelper.setValue('service_id', 'foo.bar.bas.');
        formHelper.expectError('service_id', 'rgwPattern');
      });

      it('should submit rgw with port', () => {
        formHelper.setValue('rgw_frontend_port', 1234);
        formHelper.setValue('ssl', true);
        component.onSubmit();
        expect(cephServiceService.create).toHaveBeenCalledWith({
          service_type: 'rgw',
          placement: {},
          unmanaged: false,
          rgw_frontend_port: 1234,
          rgw_frontend_ssl_certificate: '',
          rgw_frontend_ssl_key: '',
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
        component.onSubmit();
        formHelper.expectError('rgw_frontend_port', 'min');
      });

      it('should submit invalid rgw port (2)', () => {
        formHelper.setValue('rgw_frontend_port', 65536);
        component.onSubmit();
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
          placement: {},
          unmanaged: false,
          ssl: false
        });
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
        formHelper.setValue('trusted_ip_list', '  172.16.0.5,   192.1.1.10  ');
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
    });
  });
});
