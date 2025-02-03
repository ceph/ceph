import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystemsFormComponent } from './nvmeof-subsystems-form.component';
import { FormHelper } from '~/testing/unit-test-helper';
import { MAX_NAMESPACE, NvmeofService } from '~/app/shared/api/nvmeof.service';

describe('NvmeofSubsystemsFormComponent', () => {
  let component: NvmeofSubsystemsFormComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsFormComponent>;
  let nvmeofService: NvmeofService;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  const mockTimestamp = 1720693470789;
  const mockGroupName = 'default';

  beforeEach(async () => {
    spyOn(Date, 'now').and.returnValue(mockTimestamp);
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsFormComponent],
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

    fixture = TestBed.createComponent(NvmeofSubsystemsFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    form = component.subsystemForm;
    formHelper = new FormHelper(form);
    fixture.detectChanges();
    component.group = mockGroupName;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('should test form', () => {
    beforeEach(() => {
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'createSubsystem').and.stub();
    });

    it('should be creating request correctly', () => {
      const expectedNqn = 'nqn.2001-07.com.ceph:' + mockTimestamp;
      component.onSubmit();
      expect(nvmeofService.createSubsystem).toHaveBeenCalledWith({
        nqn: expectedNqn,
        max_namespaces: MAX_NAMESPACE,
        enable_ha: true,
        gw_group: mockGroupName
      });
    });

    it('should give error on invalid nqn', () => {
      formHelper.setValue('nqn', 'nqn:2001-07.com.ceph:');
      component.onSubmit();
      formHelper.expectError('nqn', 'pattern');
    });

    it('should give error on invalid max_namespaces', () => {
      formHelper.setValue('max_namespaces', -56);
      component.onSubmit();
      formHelper.expectError('max_namespaces', 'pattern');
    });

    it(`should give error on max_namespaces greater than ${MAX_NAMESPACE}`, () => {
      formHelper.setValue('max_namespaces', 2000);
      component.onSubmit();
      formHelper.expectError('max_namespaces', 'max');
    });

    it('should give error on max_namespaces lesser than 1', () => {
      formHelper.setValue('max_namespaces', 0);
      component.onSubmit();
      formHelper.expectError('max_namespaces', 'min');
    });
  });
});
