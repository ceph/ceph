import { HttpClientTestingModule } from '@angular/common/http/testing';
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
import { of } from 'rxjs';
import { PoolService } from '~/app/shared/api/pool.service';
import { NumberModule } from 'carbon-components-angular';
import { ActivatedRoute, Router } from '@angular/router';
import { By } from '@angular/platform-browser';
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
        { provide: ActivatedRoute, useValue: activatedRouteStub }
      ],
      imports: [
        HttpClientTestingModule,
        NgbTypeaheadModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        NumberModule,
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
      spyOn(nvmeofService, 'createNamespace').and.stub();
      spyOn(component, 'randomString').and.returnValue(MOCK_RANDOM_STRING);
      Object.defineProperty(router, 'url', {
        get: jasmine.createSpy('url').and.returnValue(MOCK_ROUTER.createUrl)
      });
      component.ngOnInit();
      form = component.nsForm;
      formHelper = new FormHelper(form);
    });
    it('should have set create fields correctly', () => {
      expect(component.rbdPools.length).toBe(2);
      fixture.detectChanges();
      const poolEl = fixture.debugElement.query(By.css('#pool-create')).nativeElement;
      expect(poolEl.value).toBe('rbd');
    });
    it('should create 5 namespaces correctly', () => {
      component.onSubmit();
      expect(nvmeofService.createNamespace).toHaveBeenCalledTimes(5);
      expect(nvmeofService.createNamespace).toHaveBeenCalledWith(MOCK_SUBSYSTEM, {
        gw_group: MOCK_GROUP,
        rbd_image_name: `nvme_rbd_default_${MOCK_RANDOM_STRING}`,
        rbd_pool: 'rbd',
        create_image: true,
        rbd_image_size: 1073741824
      });
    });
    it('should give error on invalid image size', () => {
      formHelper.setValue('image_size', -56);
      component.onSubmit();
      formHelper.expectError('image_size', 'pattern');
    });
    it('should give error on 0 image size', () => {
      formHelper.setValue('image_size', 0);
      component.onSubmit();
      formHelper.expectError('image_size', 'min');
    });
  });
  describe('should test edit form', () => {
    beforeEach(() => {
      router = TestBed.inject(Router);
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'getNamespace').and.returnValue(of(MOCK_NS_RESPONSE));
      spyOn(nvmeofService, 'updateNamespace').and.stub();
      Object.defineProperty(router, 'url', {
        get: jasmine.createSpy('url').and.returnValue(MOCK_ROUTER.editUrl)
      });
      fixture.detectChanges();
    });
    it('should have set edit fields correctly', () => {
      expect(nvmeofService.getNamespace).toHaveBeenCalledTimes(1);
      const poolEl = fixture.debugElement.query(By.css('#pool-edit')).nativeElement;
      expect(poolEl.disabled).toBeTruthy();
      expect(poolEl.value).toBe(MOCK_NS_RESPONSE['rbd_pool_name']);
      const sizeEl = fixture.debugElement.query(By.css('#size')).nativeElement;
      expect(sizeEl.value).toBe('1');
      const unitEl = fixture.debugElement.query(By.css('#unit')).nativeElement;
      expect(unitEl.value).toBe('GiB');
    });
    it('should not show namesapce count ', () => {
      const nsCountEl = fixture.debugElement.query(By.css('#namespace-count'));
      expect(nsCountEl).toBeFalsy();
    });
    it('should give error with no change in image size', () => {
      component.onSubmit();
      expect(component.invalidSizeError).toBe(true);
      fixture.detectChanges();
      const imageSizeInvalidEL = fixture.debugElement.query(By.css('#image-size-invalid'));
      expect(imageSizeInvalidEL).toBeTruthy();
    });
    it('should give error when size less than previous (1 GB) provided', () => {
      form = component.nsForm;
      formHelper = new FormHelper(form);
      formHelper.setValue('unit', 'MiB');
      component.onSubmit();
      expect(component.invalidSizeError).toBe(true);
      fixture.detectChanges();
      const imageSizeInvalidEL = fixture.debugElement.query(By.css('#image-size-invalid'))
        .nativeElement;
      expect(imageSizeInvalidEL).toBeTruthy();
    });
    it('should have edited namespace successfully', () => {
      component.ngOnInit();
      form = component.nsForm;
      formHelper = new FormHelper(form);
      formHelper.setValue('image_size', 2);
      component.onSubmit();
      expect(nvmeofService.updateNamespace).toHaveBeenCalledTimes(1);
      expect(nvmeofService.updateNamespace).toHaveBeenCalledWith(MOCK_SUBSYSTEM, MOCK_NSID, {
        gw_group: MOCK_GROUP,
        rbd_image_size: 2147483648
      });
    });
  });
});
