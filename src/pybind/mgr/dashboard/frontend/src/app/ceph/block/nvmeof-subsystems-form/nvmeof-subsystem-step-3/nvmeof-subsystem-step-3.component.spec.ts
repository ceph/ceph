import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystemsStepThreeComponent } from './nvmeof-subsystem-step-3.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { GridModule, InputModule, RadioModule, TagModule } from 'carbon-components-angular';
import { AUTHENTICATION } from '~/app/shared/models/nvmeof';

describe('NvmeofSubsystemsStepThreeComponent', () => {
  let component: NvmeofSubsystemsStepThreeComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsStepThreeComponent>;
  let nvmeofService: NvmeofService;
  let form: CdFormGroup;
  const mockGroupName = 'default';

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsStepThreeComponent],
      providers: [NgbActiveModal],
      imports: [
        HttpClientTestingModule,
        NgbTypeaheadModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        GridModule,
        RadioModule,
        TagModule,
        InputModule,
        ToastrModule.forRoot()
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSubsystemsStepThreeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    form = component.formGroup;
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

    describe('form initialization', () => {
      beforeEach(() => {
        fixture = TestBed.createComponent(NvmeofSubsystemsStepThreeComponent);
        component = fixture.componentInstance;

        component.stepTwoValue = {
          hostType: 'specific',
          addedHosts: ['nqn.2001-07.com.ceph:1776805137618']
        } as any;

        fixture.detectChanges();
        form = component.formGroup;
      });

      it('should initialize form with default values', () => {
        expect(form).toBeTruthy();
        expect(form.get('authType')?.value).toBe(AUTHENTICATION.Unidirectional);
        expect(form.get('subsystemDchapKey')?.value).toBe(null);
      });

      it('should keep host key optional in unidirectional mode', () => {
        const hostKeyCtrl = (form.get('hostDchapKeyList') as any).at(0).get('dhchap_key');
        hostKeyCtrl.setValue('');
        hostKeyCtrl.markAsTouched();
        hostKeyCtrl.updateValueAndValidity();

        expect(hostKeyCtrl.hasError('required')).toBeFalsy();
      });

      it('should require host key in bidirectional mode', () => {
        form.get('authType')?.setValue(AUTHENTICATION.Bidirectional);
        const hostKeyCtrl = (form.get('hostDchapKeyList') as any).at(0).get('dhchap_key');
        hostKeyCtrl.setValue('');
        hostKeyCtrl.markAsTouched();
        hostKeyCtrl.updateValueAndValidity();

        expect(hostKeyCtrl.hasError('required')).toBeTruthy();
      });

      it('should validate host key base64 format when provided', () => {
        const hostKeyCtrl = (form.get('hostDchapKeyList') as any).at(0).get('dhchap_key');
        hostKeyCtrl.setValue('not-valid-key');
        hostKeyCtrl.markAsTouched();
        hostKeyCtrl.updateValueAndValidity();

        expect(hostKeyCtrl.hasError('invalidBase64')).toBeTruthy();
      });
    });
  });
});
