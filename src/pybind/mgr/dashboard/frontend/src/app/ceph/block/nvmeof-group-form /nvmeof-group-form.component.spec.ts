import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { Router } from '@angular/router';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';

import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofGroupFormComponent } from './nvmeof-group-form.component';
import { GridModule, InputModule, SelectModule } from 'carbon-components-angular';
import { PoolService } from '~/app/shared/api/pool.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { FormHelper } from '~/testing/unit-test-helper';

describe('NvmeofGroupFormComponent', () => {
  let component: NvmeofGroupFormComponent;
  let fixture: ComponentFixture<NvmeofGroupFormComponent>;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  let poolService: PoolService;
  let taskWrapperService: TaskWrapperService;
  let cephServiceService: CephServiceService;
  let router: Router;

  const mockPools = [
    { pool_name: 'rbd', application_metadata: ['rbd'] },
    { pool_name: 'rbd', application_metadata: ['rbd'] },
    { pool_name: 'pool2', application_metadata: ['rgw'] }
  ];

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
        GridModule,
        InputModule,
        SelectModule,
        ToastrModule.forRoot()
      ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGroupFormComponent);
    component = fixture.componentInstance;
    poolService = TestBed.inject(PoolService);
    taskWrapperService = TestBed.inject(TaskWrapperService);
    cephServiceService = TestBed.inject(CephServiceService);
    router = TestBed.inject(Router);

    spyOn(poolService, 'list').and.returnValue(Promise.resolve(mockPools));

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

    it('should require pool', () => {
      formHelper.setValue('pool', null);
      formHelper.expectError('pool', 'required');
    });

    it('should be valid when groupName and pool are set', () => {
      formHelper.setValue('groupName', 'test-group');
      formHelper.setValue('pool', 'rbd');
      expect(form.valid).toBe(true);
    });
  });

  describe('loadPools', () => {
    it('should load pools and filter by rbd application metadata', fakeAsync(() => {
      component.loadPools();
      tick();
      expect(component.pools.length).toBe(2);
      expect(component.pools.map((p) => p.pool_name)).toEqual(['rbd', 'rbd']);
    }));

    it('should set default pool to rbd if available', fakeAsync(() => {
      component.groupForm.get('pool').setValue(null);
      component.loadPools();
      tick();
      expect(component.groupForm.get('pool').value).toBe('rbd');
    }));

    it('should set first pool if rbd is not available', fakeAsync(() => {
      component.groupForm.get('pool').setValue(null);
      const poolsWithoutRbd = [{ pool_name: 'custom-pool', application_metadata: ['rbd'] }];
      (poolService.list as jasmine.Spy).and.returnValue(Promise.resolve(poolsWithoutRbd));
      component.loadPools();
      tick();
      expect(component.groupForm.get('pool').value).toBe('custom-pool');
    }));

    it('should handle empty pools', fakeAsync(() => {
      (poolService.list as jasmine.Spy).and.returnValue(Promise.resolve([]));
      component.loadPools();
      tick();
      expect(component.pools.length).toBe(0);
      expect(component.poolsLoading).toBe(false);
    }));

    it('should handle pool loading error', fakeAsync(() => {
      (poolService.list as jasmine.Spy).and.returnValue(Promise.reject('error'));
      component.loadPools();
      tick();
      expect(component.pools).toEqual([]);
      expect(component.poolsLoading).toBe(false);
    }));
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
      component.groupForm.get('pool').setValue('rbd');
      component.onSubmit();

      expect(cephServiceService.create).not.toHaveBeenCalled();
    });

    it('should create service with correct spec', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [{ hostname: 'host1' }, { hostname: 'host2' }],
        getSelectedHostnames: (): string[] => ['host1', 'host2']
      } as any;

      component.groupForm.get('groupName').setValue('defalut');
      component.groupForm.get('pool').setValue('rbd');
      component.groupForm.get('unmanaged').setValue(false);
      component.onSubmit();

      expect(cephServiceService.create).toHaveBeenCalledWith({
        service_type: 'nvmeof',
        service_id: 'rbd.defalut',
        pool: 'rbd',
        group: 'defalut',
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
      component.groupForm.get('pool').setValue('rbd');
      component.groupForm.get('unmanaged').setValue(true);
      component.onSubmit();

      expect(cephServiceService.create).toHaveBeenCalledWith(
        jasmine.objectContaining({
          unmanaged: true,
          group: 'unmanaged-group',
          pool: 'rbd'
        })
      );
    });

    it('should navigate to list view on success', () => {
      component.gatewayNodeComponent = {
        getSelectedHosts: (): any[] => [{ hostname: 'host1' }],
        getSelectedHostnames: (): string[] => ['host1']
      } as any;

      component.groupForm.get('groupName').setValue('test-group');
      component.groupForm.get('pool').setValue('rbd');
      component.onSubmit();

      expect(router.navigateByUrl).toHaveBeenCalledWith('/block/nvmeof/gateways');
    });
  });
});
