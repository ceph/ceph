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

describe('NvmeofNamespacesFormComponent', () => {
  let component: NvmeofNamespacesFormComponent;
  let fixture: ComponentFixture<NvmeofNamespacesFormComponent>;
  let nvmeofService: NvmeofService;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  const mockRandomString = 1720693470789;
  const mockSubsystemNQN = 'nqn.2021-11.com.example:subsystem';
  const mockGWgroup = 'default';

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofNamespacesFormComponent],
      providers: [NgbActiveModal, { provide: PoolService, useClass: MockPoolService }],
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
    component.ngOnInit();
    form = component.nsForm;
    formHelper = new FormHelper(form);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('should test form', () => {
    beforeEach(() => {
      component.subsystemNQN = mockSubsystemNQN;
      component.group = mockGWgroup;
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'createNamespace').and.stub();
      spyOn(component, 'randomString').and.returnValue(mockRandomString);
    });
    it('should create 5 namespaces correctly', () => {
      component.onSubmit();
      expect(nvmeofService.createNamespace).toHaveBeenCalledTimes(5);
      expect(nvmeofService.createNamespace).toHaveBeenCalledWith(mockSubsystemNQN, {
        gw_group: mockGWgroup,
        rbd_image_name: `nvme_rbd_default_${mockRandomString}`,
        rbd_pool: 'rbd',
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
});
