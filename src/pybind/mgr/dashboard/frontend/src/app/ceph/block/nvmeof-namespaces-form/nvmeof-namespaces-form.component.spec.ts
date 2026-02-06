import { HttpClientTestingModule } from '@angular/common/http/testing';
import { HttpResponse } from '@angular/common/http';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofNamespacesFormComponent } from './nvmeof-namespaces-form.component';
import { FormHelper, Mocks } from '~/testing/unit-test-helper';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { of, Observable } from 'rxjs';
import { PoolService } from '~/app/shared/api/pool.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NumberModule, RadioModule, ComboBoxModule, SelectModule } from 'carbon-components-angular';
import { ActivatedRoute, Router } from '@angular/router';
import { By } from '@angular/platform-browser';
import { ActivatedRouteStub } from '~/testing/activated-route-stub';
import { NvmeofInitiatorCandidate } from '~/app/shared/models/nvmeof';

const MOCK_POOLS = [
  Mocks.getPool('pool-1', 1, ['cephfs']),
  Mocks.getPool('rbd', 2),
  Mocks.getPool('pool-2', 3)
];
class MockPoolsService {
  getList() {
    return of(MOCK_POOLS);
  }
}

class MockTaskWrapperService {
  wrapTaskAroundCall(args: { task: any; call: Observable<any> }) {
    return args.call;
  }
}

const MOCK_NS_RESPONSE = {
  nsid: 1,
  uuid: '185d541f-76bf-45b5-b445-f71829346c38',
  bdev_name: 'bdev_185d541f-76bf-45b5-b445-f71829346c38',
  rbd_image_name: 'nvme_rbd_default_sscfagwuvvr',
  rbd_pool_name: 'rbd',
  load_balancing_group: 1,
  rbd_image_size: '1073741824',
  block_size: 512,
  rw_ios_per_second: '0',
  rw_mbytes_per_second: '0',
  r_mbytes_per_second: '0',
  w_mbytes_per_second: '0',
  trash_image: false
};

const MOCK_ROUTER = {
  editUrl:
    'https://192.168.100.100:8443/#/block/nvmeof/subsystems/(modal:edit/nqn.2001-07.com.ceph:1744881547418.default/namespace/1)?group=default',
  createUrl: 'https://192.168.100.100:8443/#/block/nvmeof/subsystems/(modal:create)?group=default'
};

describe('NvmeofNamespacesFormComponent', () => {
  let component: NvmeofNamespacesFormComponent;
  let fixture: ComponentFixture<NvmeofNamespacesFormComponent>;
  let nvmeofService: NvmeofService;
  let router: Router;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  let activatedRouteStub: ActivatedRouteStub;
  const MOCK_RANDOM_STRING = 1720693470789;
  const MOCK_SUBSYSTEM = 'nqn.2021-11.com.example:subsystem';
  const MOCK_GROUP = 'default';
  const MOCK_NSID = String(MOCK_NS_RESPONSE['nsid']);

  beforeEach(async () => {
    activatedRouteStub = new ActivatedRouteStub(
      { subsystem_nqn: MOCK_SUBSYSTEM, nsid: MOCK_NSID },
      { group: MOCK_GROUP }
    );
    await TestBed.configureTestingModule({
      declarations: [NvmeofNamespacesFormComponent],
      providers: [
        NgbActiveModal,
        { provide: PoolService, useClass: MockPoolsService },
        { provide: ActivatedRoute, useValue: activatedRouteStub },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService }
      ],
      imports: [
        HttpClientTestingModule,
        NgbTypeaheadModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        NumberModule,
        RadioModule,
        ComboBoxModule,
        SelectModule,
        ToastrModule.forRoot()
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(NvmeofNamespacesFormComponent);
    component = fixture.componentInstance;
  });

  it('should create component', () => {
    expect(component).toBeTruthy();
  });
  describe('should test create form', () => {
    beforeEach(() => {
      router = TestBed.inject(Router);
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'createNamespace').and.returnValue(
        of(new HttpResponse({ body: MOCK_NS_RESPONSE }))
      );
      spyOn(nvmeofService, 'addNamespaceInitiators').and.returnValue(of({}));
      spyOn(nvmeofService, 'getInitiators').and.returnValue(
        of([{ nqn: 'host1' }, { nqn: 'host2' }])
      );
      spyOn(component, 'randomString').and.returnValue(MOCK_RANDOM_STRING);
      Object.defineProperty(router, 'url', {
        get: jasmine.createSpy('url').and.returnValue(MOCK_ROUTER.createUrl)
      });
      component.ngOnInit();
      form = component.nsForm;
      formHelper = new FormHelper(form);
      formHelper.setValue('pool', 'rbd');
    });
    it('should create 5 namespaces correctly', () => {
      formHelper.setValue('pool', 'rbd');
      formHelper.setValue('image_size', 1073741824);
      formHelper.setValue('subsystem', MOCK_SUBSYSTEM);
      component.onSubmit();
      expect(nvmeofService.createNamespace).toHaveBeenCalledTimes(5);
      expect(nvmeofService.createNamespace).toHaveBeenCalledWith(MOCK_SUBSYSTEM, {
        gw_group: MOCK_GROUP,
        rbd_image_name: `nvme_rbd_default_${MOCK_RANDOM_STRING}`,
        rbd_pool: 'rbd',
        create_image: true,
        rbd_image_size: 1073741824,
        no_auto_visible: false
      });
    });

    it('should create multiple namespaces with suffixed custom image names', () => {
      formHelper.setValue('pool', 'rbd');
      formHelper.setValue('image_size', 1073741824);
      formHelper.setValue('subsystem', MOCK_SUBSYSTEM);
      formHelper.setValue('nsCount', 2);
      formHelper.setValue('rbd_image_name', 'test-img');
      component.onSubmit();
      expect(nvmeofService.createNamespace).toHaveBeenCalledTimes(2);
      expect((nvmeofService.createNamespace as any).calls.argsFor(0)[1].rbd_image_name).toBe(
        'test-img-1'
      );
      expect((nvmeofService.createNamespace as any).calls.argsFor(1)[1].rbd_image_name).toBe(
        'test-img-2'
      );
    });
    it('should give error on invalid image size', () => {
      formHelper.setValue('image_size', -56);
      component.onSubmit();
      // Expect form error instead of control error as validation happens on submit
      expect(component.nsForm.hasError('cdSubmitButton')).toBeTruthy();
    });
    it('should give error on 0 image size', () => {
      formHelper.setValue('image_size', 0);
      component.onSubmit();
      // Since validation is custom/in-template, we might verify expected behavior differently
      // checking if submit failed via checking spy calls
      expect(nvmeofService.createNamespace).not.toHaveBeenCalled();
      expect(component.nsForm.hasError('cdSubmitButton')).toBeTruthy();
    });

    it('should require initiators when host access is specific', () => {
      formHelper.setValue('host_access', 'specific');
      formHelper.expectError('initiators', 'required');
      formHelper.setValue('initiators', ['host1']);
      formHelper.expectValid('initiators');
    });

    it('should call addNamespaceInitiators on submit with specific hosts', () => {
      formHelper.setValue('pool', 'rbd');
      formHelper.setValue('image_size', 1073741824);
      formHelper.setValue('subsystem', MOCK_SUBSYSTEM);
      formHelper.setValue('host_access', 'specific');
      formHelper.setValue('initiators', ['host1']);
      component.onSubmit();
      expect(nvmeofService.createNamespace).toHaveBeenCalled();
      // Wait for async operations if needed, or check if mocking is correct
      expect(nvmeofService.addNamespaceInitiators).toHaveBeenCalledTimes(5); // 5 namespaces created by default
      expect(nvmeofService.addNamespaceInitiators).toHaveBeenCalledWith(1, {
        gw_group: MOCK_GROUP,
        subsystem_nqn: MOCK_SUBSYSTEM,
        host_nqn: 'host1'
      });
    });

    it('should update initiators form control on selection', () => {
      const mockEvent: NvmeofInitiatorCandidate[] = [
        { content: 'host1', selected: true },
        { content: 'host2', selected: true }
      ];
      component.onInitiatorSelection(mockEvent);
      expect(component.nsForm.get('initiators').value).toEqual(['host1', 'host2']);
      expect(component.nsForm.get('initiators').dirty).toBe(true);
    });
  });
  describe('should test edit form', () => {
    beforeEach(() => {
      router = TestBed.inject(Router);
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'getNamespace').and.returnValue(of(MOCK_NS_RESPONSE));
      spyOn(nvmeofService, 'updateNamespace').and.returnValue(
        of(new HttpResponse({ status: 200 }))
      );
      Object.defineProperty(router, 'url', {
        get: jasmine.createSpy('url').and.returnValue(MOCK_ROUTER.editUrl)
      });
      fixture.detectChanges();
    });

    it('should have set edit fields correctly', () => {
      expect(nvmeofService.getNamespace).toHaveBeenCalledTimes(1);
      expect(component.nsForm.get('pool').disabled).toBeTruthy();
      expect(component.nsForm.get('pool').value).toBe(MOCK_NS_RESPONSE['rbd_pool_name']);
      // Size formatted by pipe
      expect(component.nsForm.get('image_size').value).toBe('1 GiB');
    });

    it('should not show namespace count', () => {
      const nsCountEl = fixture.debugElement.query(By.css('cds-number[formControlName="nsCount"]'));
      expect(nsCountEl).toBeFalsy();
    });

    it('should give error with no change in image size', () => {
      component.nsForm.get('image_size').updateValueAndValidity();
      expect(component.nsForm.get('image_size').hasError('minSize')).toBe(true);
    });

    it('should give error when size less than previous (1 GB) provided', () => {
      form = component.nsForm;
      formHelper = new FormHelper(form);
      formHelper.setValue('image_size', '512 MiB'); // Less than 1 GiB
      component.nsForm.get('image_size').updateValueAndValidity();
      expect(component.nsForm.get('image_size').hasError('minSize')).toBe(true);
    });

    it('should have edited namespace successfully', () => {
      component.ngOnInit();
      form = component.nsForm;
      formHelper = new FormHelper(form);
      formHelper.setValue('image_size', '2 GiB');
      component.onSubmit();
      expect(nvmeofService.updateNamespace).toHaveBeenCalledTimes(1);
      expect(nvmeofService.updateNamespace).toHaveBeenCalledWith(MOCK_SUBSYSTEM, MOCK_NSID, {
        gw_group: MOCK_GROUP,
        rbd_image_size: 2147483648
      });
    });
  });
});
