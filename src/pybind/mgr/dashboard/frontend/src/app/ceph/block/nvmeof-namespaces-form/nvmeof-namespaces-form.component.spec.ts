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
import { of, Observable } from 'rxjs';
import { PoolService } from '~/app/shared/api/pool.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NumberModule, RadioModule, ComboBoxModule, SelectModule } from 'carbon-components-angular';
import { ActivatedRoute, Router } from '@angular/router';

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
    it('should call createNamespace on submit with specific hosts', () => {
      formHelper.setValue('pool', 'rbd');
      formHelper.setValue('image_size', new FormatterService().toBytes('1GiB'));
      formHelper.setValue('subsystem', MOCK_SUBSYSTEM);
      formHelper.setValue('host_access', 'specific');
      formHelper.setValue('initiators', ['host1']);
      component.onSubmit();
      expect(nvmeofService.createNamespace).toHaveBeenCalled();
    });
  });
});
