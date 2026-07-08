import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';

import { of } from 'rxjs';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofGroupFormComponent } from './nvmeof-group-form.component';
import { CheckboxModule, GridModule, InputModule, SelectModule } from 'carbon-components-angular';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { FormHelper } from '~/testing/unit-test-helper';

describe('NvmeofGroupFormComponent', () => {
  let component: NvmeofGroupFormComponent;
  let fixture: ComponentFixture<NvmeofGroupFormComponent>;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  let taskWrapperService: TaskWrapperService;
  let cephServiceService: CephServiceService;
  let nvmeofService: NvmeofService;
  let router: Router;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofGroupFormComponent],
      providers: [NgbActiveModal],
      imports: [
        HttpClientTestingModule,
        NgbTypeaheadModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        CheckboxModule,
        GridModule,
        InputModule,
        SelectModule
      ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    })
      .overrideTemplate(NvmeofGroupFormComponent, '')
      .compileComponents();

    fixture = TestBed.createComponent(NvmeofGroupFormComponent);
    component = fixture.componentInstance;
    taskWrapperService = TestBed.inject(TaskWrapperService);
    cephServiceService = TestBed.inject(CephServiceService);
    nvmeofService = TestBed.inject(NvmeofService);
    router = TestBed.inject(Router);

    // Mock NvmeofService.exists so the async unique validator resolves immediately
    spyOn(nvmeofService, 'exists').and.returnValue(of(false));

    component.ngOnInit();
    form = component.groupForm;
    formHelper = new FormHelper(form);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize form with empty fields', () => {
    expect(form.controls.groupName.value).toBeNull();
    expect(form.controls.unmanaged.value).toBe(false);
    expect(form.controls.enableEncryption.value).toBe(false);
    expect(form.controls.certificateType.value).toBe('internal');
  });

  it('should set action to CREATE on init', () => {
    expect(component.action).toBe('Create');
  });

  it('should set resource to gateway group', () => {
    expect(component.resource).toBe('gateway group');
  });

  describe('form validation', () => {
    it('should require groupName', () => {
      formHelper.setValue('groupName', '');
      formHelper.expectError('groupName', 'required');
    });

    it('should be valid when groupName is set', () => {
      formHelper.setValue('groupName', 'test-group');
      expect(form.controls.groupName.valid).toBe(true);
    });

    it('should validate groupName for invalid characters', () => {
      formHelper.setValue('groupName', 'test@group');
      formHelper.expectError('groupName', 'invalidChars');
    });
  });

  describe('onSubmit', () => {
    beforeEach(() => {
      spyOn(cephServiceService, 'create').and.returnValue(of({}));
      spyOn(taskWrapperService, 'wrapTaskAroundCall').and.callFake(({ call }) => call);
      spyOn(router, 'navigateByUrl');
    });

    it('should not call create if no hosts are selected', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [],
        getSelectedHostnames: (): string[] => []
      } as any;

      component.groupForm.get('groupName').setValue('test-group');
      component.onSubmit();

      expect(cephServiceService.create).not.toHaveBeenCalled();
    });

    it('should create service with correct spec', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [{ hostname: 'host1' }, { hostname: 'host2' }],
        getSelectedHostnames: (): string[] => ['host1', 'host2']
      } as any;

      component.groupForm.get('groupName').setValue('default');
      component.groupForm.get('unmanaged').setValue(false);
      component.onSubmit();

      expect(cephServiceService.create).toHaveBeenCalledWith({
        service_type: 'nvmeof',
        service_id: 'nvmeof.default',
        group: 'default',
        placement: {
          hosts: ['host1', 'host2']
        },
        unmanaged: false
      });
    });

    it('should create service with unmanaged flag set to true', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [{ hostname: 'host1' }],
        getSelectedHostnames: (): string[] => ['host1']
      } as any;

      component.groupForm.get('groupName').setValue('unmanaged-group');
      component.groupForm.get('unmanaged').setValue(true);
      component.onSubmit();

      expect(cephServiceService.create).toHaveBeenCalledWith(
        jasmine.objectContaining({
          unmanaged: true,
          group: 'unmanaged-group'
        })
      );
    });

    it('should create service with encryption key when enabled', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [{ hostname: 'host1' }],
        getSelectedHostnames: (): string[] => ['host1']
      } as any;

      component.groupForm.get('groupName').setValue('encrypted-group');
      component.groupForm.get('enable_auth').setValue(true);
      component.groupForm.get('encryptionKey').setValue('encryption-key-123');
      component.onSubmit();

      expect(cephServiceService.create).toHaveBeenCalledWith(
        jasmine.objectContaining({
          group: 'encrypted-group',
          encryption_key: 'encryption-key-123'
        })
      );
    });

    it('should create service with cephadm-signed mTLS when internal selected', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [{ hostname: 'host1' }],
        getSelectedHostnames: (): string[] => ['host1']
      } as any;

      component.groupForm.get('groupName').setValue('mtls-internal');
      component.groupForm.get('enableEncryption').setValue(true);
      component.groupForm.get('encryptionKey').setValue('test-encryption-key');
      component.groupForm.get('enableMtls').setValue(true);
      component.groupForm.get('certificateType').setValue(component.CertificateType.internal);
      component.groupForm.get('custom_sans').setValue(['gw1.local', '192.168.0.10']);

      component.onSubmit();

      expect(cephServiceService.create).toHaveBeenCalledWith(
        jasmine.objectContaining({
          service_type: 'nvmeof',
          service_id: 'nvmeof.mtls-internal',
          ssl: true,
          enable_auth: true,
          certificate_source: 'cephadm-signed',
          custom_sans: ['gw1.local', '192.168.0.10']
        })
      );
    });

    it('should create service with inline mTLS when external selected', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [{ hostname: 'host1' }],
        getSelectedHostnames: (): string[] => ['host1']
      } as any;

      component.groupForm.get('groupName').setValue('mtls-external');
      component.groupForm.get('enableEncryption').setValue(true);
      component.groupForm.get('encryptionKey').setValue('test-encryption-key');
      component.groupForm.get('enableMtls').setValue(true);
      component.groupForm.get('certificateType').setValue(component.CertificateType.external);
      component.groupForm.get('rootCACert').setValue('root');
      component.groupForm.get('clientCert').setValue('client-cert');
      component.groupForm.get('clientKey').setValue('client-key');
      component.groupForm.get('serverCert').setValue('server-cert');
      component.groupForm.get('serverKey').setValue('server-key');

      component.onSubmit();

      expect(cephServiceService.create).toHaveBeenCalledWith(
        jasmine.objectContaining({
          service_id: 'nvmeof.mtls-external',
          ssl: true,
          enable_auth: true,
          certificate_source: 'inline',
          root_ca_cert: 'root',
          client_cert: 'client-cert',
          client_key: 'client-key',
          server_cert: 'server-cert',
          server_key: 'server-key'
        })
      );
    });

    it('should navigate to list view on success', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [{ hostname: 'host1' }],
        getSelectedHostnames: (): string[] => ['host1']
      } as any;

      component.groupForm.get('groupName').setValue('test-group');
      component.onSubmit();

      expect(router.navigateByUrl).toHaveBeenCalledWith('/block/nvmeof/gateways');
    });
  });
});
