import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';

import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofGroupFormComponent } from './nvmeof-group-form.component';
import { CheckboxModule, GridModule, InputModule, SelectModule } from 'carbon-components-angular';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { FormHelper } from '~/testing/unit-test-helper';

describe('NvmeofGroupFormComponent', () => {
  let component: NvmeofGroupFormComponent;
  let fixture: ComponentFixture<NvmeofGroupFormComponent>;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  let taskWrapperService: TaskWrapperService;
  let cephServiceService: CephServiceService;
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
        SelectModule,
        ToastrModule.forRoot()
      ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGroupFormComponent);
    component = fixture.componentInstance;
    taskWrapperService = TestBed.inject(TaskWrapperService);
    cephServiceService = TestBed.inject(CephServiceService);
    router = TestBed.inject(Router);

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
        service_id: 'default',
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

    it('should create service with encryption when enabled', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [{ hostname: 'host1' }],
        getSelectedHostnames: (): string[] => ['host1']
      } as any;

      component.groupForm.get('groupName').setValue('encrypted-group');
      component.groupForm.get('enableEncryption').setValue(true);
      component.groupForm.get('encryptionConfig').setValue('encryption-key-123');
      component.onSubmit();

      expect(cephServiceService.create).toHaveBeenCalledWith(
        jasmine.objectContaining({
          group: 'encrypted-group',
          encryption_key: 'encryption-key-123'
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
