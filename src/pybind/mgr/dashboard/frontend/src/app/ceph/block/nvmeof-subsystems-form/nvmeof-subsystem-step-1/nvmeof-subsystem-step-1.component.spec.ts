import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystemsStepOneComponent } from './nvmeof-subsystem-step-1.component';
import { FormHelper } from '~/testing/unit-test-helper';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { ComboBoxModule, GridModule, InputModule, RadioModule } from 'carbon-components-angular';

import { of } from 'rxjs';

describe('NvmeofSubsystemsStepOneComponent', () => {
  let component: NvmeofSubsystemsStepOneComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsStepOneComponent>;

  let nvmeofService: NvmeofService;

  let form: CdFormGroup;
  let formHelper: FormHelper;
  const mockGroupName = 'default';

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsStepOneComponent],
      imports: [
        HttpClientTestingModule,
        SharedModule,
        ReactiveFormsModule,
        RouterTestingModule,
        NgbTypeaheadModule,
        InputModule,
        GridModule,
        ComboBoxModule,
        RadioModule
      ],
      providers: [NgbActiveModal],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofSubsystemsStepOneComponent);
    component = fixture.componentInstance;

    nvmeofService = TestBed.inject(NvmeofService);
    spyOn(nvmeofService, 'getHostsForGroup').and.returnValue(of([]));
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
      formHelper.expectError('nqn', 'nqnPattern');
    });

    it('should require subnet mask when auto-fetch is selected and next is clicked', () => {
      formHelper.setValue('listenerMode', component.LISTENER_MODE.AUTO_FETCH);
      formHelper.setValue('subnetMask', '');

      form.get('subnetMask')?.markAsTouched();
      form.get('subnetMask')?.markAsDirty();
      form.updateValueAndValidity();
      fixture.detectChanges();

      expect(form.get('subnetMask')?.hasError('required')).toBeTruthy();
    });
  });
});
