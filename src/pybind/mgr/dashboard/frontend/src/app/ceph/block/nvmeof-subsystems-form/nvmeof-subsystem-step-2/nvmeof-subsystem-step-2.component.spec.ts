import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystemsStepTwoComponent } from './nvmeof-subsystem-step-2.component';
import { FormHelper } from '~/testing/unit-test-helper';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';

describe('NvmeofSubsystemsStepTwoComponent', () => {
  let component: NvmeofSubsystemsStepTwoComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsStepTwoComponent>;
  let nvmeofService: NvmeofService;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  const mockGroupName = 'default';

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsStepTwoComponent],
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

    fixture = TestBed.createComponent(NvmeofSubsystemsStepTwoComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    form = component.formGroup;
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

    it('should give error on invalid nqn', () => {
      formHelper.setValue('nqn', 'nqn:2001-07.com.ceph:');
      formHelper.expectError('nqn', 'pattern');
    });
  });
});
