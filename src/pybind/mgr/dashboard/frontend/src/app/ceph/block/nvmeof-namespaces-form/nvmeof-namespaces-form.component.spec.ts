import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofNamespacesFormComponent } from './nvmeof-namespaces-form.component';
import { FormHelper } from '~/testing/unit-test-helper';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';

describe('NvmeofNamespacesFormComponent', () => {
  let component: NvmeofNamespacesFormComponent;
  let fixture: ComponentFixture<NvmeofNamespacesFormComponent>;
  let nvmeofService: NvmeofService;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  const mockTimestamp = 1720693470789;
  const mockSubsystemNQN = 'nqn.2021-11.com.example:subsystem';

  beforeEach(async () => {
    spyOn(Date, 'now').and.returnValue(mockTimestamp);
    await TestBed.configureTestingModule({
      declarations: [NvmeofNamespacesFormComponent],
      providers: [NgbActiveModal],
      imports: [
        HttpClientTestingModule,
        NgbTypeaheadModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
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
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'createNamespace').and.stub();
    });

    it('should be creating request correctly', () => {
      const image = 'nvme_ns_image:' + mockTimestamp;
      component.onSubmit();
      expect(nvmeofService.createNamespace).toHaveBeenCalledWith(mockSubsystemNQN, {
        rbd_image_name: image,
        rbd_pool: null,
        size: 1073741824
      });
    });

    it('should give error on invalid image name', () => {
      formHelper.setValue('image', '/ghfhdlk;kd;@');
      component.onSubmit();
      formHelper.expectError('image', 'pattern');
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
