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
import { FormatterService } from '~/app/shared/services/formatter.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { of, Observable } from 'rxjs';
import { PoolService } from '~/app/shared/api/pool.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NumberModule, RadioModule, ComboBoxModule, SelectModule } from 'carbon-components-angular';
import { ActivatedRoute, Router } from '@angular/router';
import { RadosNamespace } from '~/app/shared/models/nvmeof';

import { ActivatedRouteStub } from '~/testing/activated-route-stub';

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
  rbd_image_size: new FormatterService().toBytes('1GiB').toString(),
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

const MOCK_RADOS_NAMESPACES: RadosNamespace[] = [
  { namespace: 'tenant-1', num_images: 2 },
  { namespace: 'tenant-2', num_images: 0 }
];

const MOCK_RBD_IMAGES_DEFAULT_NS = [
  { name: 'img-alpha', size: 1073741824 },
  { name: 'img-beta', size: 2147483648 }
];

const MOCK_RBD_IMAGES_TENANT1 = [
  { name: 'img-alpha', size: 1073741824 }, // same name as in default ns — must stay selectable
  { name: 'img-gamma', size: 536870912 }
];

/**
 * Builds a minimal namespace shape for fetchUsedImages() tests.
 */
function makeUsedNs(pool: string, imageName: string, radosNs: string = '') {
  return {
    nsid: 1,
    uuid: 'uuid',
    bdev_name: 'bdev',
    rbd_image_name: imageName,
    rbd_pool_name: pool,
    rados_namespace_name: radosNs,
    load_balancing_group: 1,
    rbd_image_size: 1073741824,
    block_size: 512,
    rw_ios_per_second: 0,
    rw_mbytes_per_second: 0,
    r_mbytes_per_second: 0,
    w_mbytes_per_second: 0
  };
}

describe('NvmeofNamespacesFormComponent', () => {
  let component: NvmeofNamespacesFormComponent;
  let fixture: ComponentFixture<NvmeofNamespacesFormComponent>;
  let nvmeofService: NvmeofService;
  let rbdService: RbdService;
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

  describe('create form', () => {
    beforeEach(() => {
      router = TestBed.inject(Router);
      nvmeofService = TestBed.inject(NvmeofService);
      rbdService = TestBed.inject(RbdService);

      spyOn(nvmeofService, 'createNamespace').and.returnValue(
        of(new HttpResponse({ body: MOCK_NS_RESPONSE }))
      );
      spyOn(nvmeofService, 'getInitiators').and.returnValue(
        of([{ nqn: 'host1' }, { nqn: 'host2' }])
      );
      spyOn(rbdService, 'listNamespaces').and.returnValue(of(MOCK_RADOS_NAMESPACES));
      // Default: no existing namespaces. Individual tests can override via
      // (nvmeofService.listNamespaces as jasmine.Spy).and.returnValue(...)
      spyOn(nvmeofService, 'listNamespaces').and.returnValue(of({ namespaces: [] }));
      spyOn(component, 'randomString').and.returnValue(MOCK_RANDOM_STRING);

      Object.defineProperty(router, 'url', {
        get: jasmine.createSpy('url').and.returnValue(MOCK_ROUTER.createUrl)
      });

      component.ngOnInit();
      form = component.nsForm;
      formHelper = new FormHelper(form);
    });

    it('should call createNamespace on submit with specific hosts', () => {
      formHelper.setValue('pool', 'rbd');
      formHelper.setValue('image_size', new FormatterService().toBytes('1GiB'));
      formHelper.setValue('subsystem', MOCK_SUBSYSTEM);
      formHelper.setValue('host_access', 'specific');
      formHelper.setValue('initiators', ['host1']);
      component.onSubmit();
      expect(nvmeofService.createNamespace).toHaveBeenCalled();
    });

    it('should not send block_size from namespace_size UI field', () => {
      formHelper.setValue('pool', 'rbd');
      formHelper.setValue('image_size', new FormatterService().toBytes('1GiB'));
      formHelper.setValue('subsystem', MOCK_SUBSYSTEM);
      formHelper.setValue('namespace_size', 10);

      component.onSubmit();

      const request = (nvmeofService.createNamespace as jasmine.Spy).calls.mostRecent().args[1];
      expect(request.block_size).toBeUndefined();
    });

    describe('RADOS namespace field', () => {
      it('should load RADOS namespaces when a pool is selected', () => {
        formHelper.setValue('pool', 'rbd');

        expect(rbdService.listNamespaces).toHaveBeenCalledWith('rbd');
        expect(component.radosNamespaces).toEqual(MOCK_RADOS_NAMESPACES);
      });

      it('should clear rados_namespace selection and reload namespaces when pool changes', () => {
        formHelper.setValue('pool', 'rbd');
        formHelper.setValue('rados_namespace', 'tenant-1');

        // Switching to a different pool must clear the selected namespace
        formHelper.setValue('pool', 'pool-2');

        expect(form.getValue('rados_namespace')).toBeNull();
        // listNamespaces should have been called for the new pool
        expect(rbdService.listNamespaces).toHaveBeenCalledWith('pool-2');
      });

      it('should not include rados_namespace in the create request when none is selected', () => {
        formHelper.setValue('pool', 'rbd');
        formHelper.setValue('image_size', new FormatterService().toBytes('1GiB'));
        formHelper.setValue('subsystem', MOCK_SUBSYSTEM);

        component.onSubmit();

        const request = (nvmeofService.createNamespace as jasmine.Spy).calls.mostRecent().args[1];
        expect(request.rados_namespace).toBeUndefined();
      });

      it('should include rados_namespace in the create request when a namespace is selected', () => {
        formHelper.setValue('pool', 'rbd');
        formHelper.setValue('rados_namespace', 'tenant-1');
        formHelper.setValue('image_size', new FormatterService().toBytes('1GiB'));
        formHelper.setValue('subsystem', MOCK_SUBSYSTEM);

        component.onSubmit();

        const request = (nvmeofService.createNamespace as jasmine.Spy).calls.mostRecent().args[1];
        expect(request.rados_namespace).toBe('tenant-1');
      });
    });

    describe('image filtering with RADOS namespace isolation', () => {
      /**
       * The NvmeofService and RbdService are singletons in TestBed DI.  The parent
       * beforeEach has already called spyOn() on createNamespace, getInitiators and
       * listNamespaces (RBD). Here we only need to override the two methods that
       * control which images are "used" and which are available: listNamespaces and
       * rbdService.list.  We create a new component instance so that ngOnInit() picks
       * up the configured listNamespaces stub when it calls fetchUsedImages().
       */
      function buildComponentWithStubs(
        usedNamespaces: ReturnType<typeof makeUsedNs>[],
        rbdListResponse: { pool_name: string; value: { name: string; size: number }[] }[]
      ): void {
        // Re-spy the two methods that vary between test cases
        (nvmeofService.listNamespaces as jasmine.Spy).and.returnValue(
          of({ namespaces: usedNamespaces })
        );

        fixture = TestBed.createComponent(NvmeofNamespacesFormComponent);
        component = fixture.componentInstance;

        // rbdService is injected into the component — spy before ngOnInit
        spyOn(component['rbdService'], 'list').and.returnValue(of(rbdListResponse));

        component.ngOnInit();
        form = component.nsForm;
        formHelper = new FormHelper(form);
      }

      it('should treat the same image name in different RADOS namespaces as independently available', () => {
        // 'img-alpha' is recorded as used in the **default** namespace of pool 'rbd'
        buildComponentWithStubs(
          [makeUsedNs('rbd', 'img-alpha', '')],
          [{ pool_name: 'rbd', value: MOCK_RBD_IMAGES_TENANT1 }]
        );

        formHelper.setValue('pool', 'rbd');
        formHelper.setValue('rados_namespace', 'tenant-1');

        // img-alpha exists in tenant-1's image list AND is NOT used in tenant-1,
        // so it must remain available for selection.
        expect(component.rbdImages.map((i) => i.name)).toContain('img-alpha');
      });

      it('should exclude images already used in the same pool + RADOS namespace', () => {
        // 'img-alpha' is recorded as used in 'tenant-1'
        buildComponentWithStubs(
          [makeUsedNs('rbd', 'img-alpha', 'tenant-1')],
          [{ pool_name: 'rbd', value: MOCK_RBD_IMAGES_TENANT1 }]
        );

        formHelper.setValue('pool', 'rbd');
        formHelper.setValue('rados_namespace', 'tenant-1');

        expect(component.rbdImages.map((i) => i.name)).not.toContain('img-alpha');
        expect(component.rbdImages.map((i) => i.name)).toContain('img-gamma');
      });

      it('should exclude used images in the default namespace when no RADOS namespace is selected', () => {
        // 'img-alpha' is recorded as used in the default namespace (empty radosNs)
        buildComponentWithStubs(
          [makeUsedNs('rbd', 'img-alpha', '')],
          [{ pool_name: 'rbd', value: MOCK_RBD_IMAGES_DEFAULT_NS }]
        );

        formHelper.setValue('pool', 'rbd');
        // rados_namespace stays null → scoped to the default namespace

        expect(component.rbdImages.map((i) => i.name)).not.toContain('img-alpha');
        expect(component.rbdImages.map((i) => i.name)).toContain('img-beta');
      });
    });
  });
});
