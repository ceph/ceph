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
import { GridModule, RadioModule, TagModule } from 'carbon-components-angular';
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
      it('should initialize form with default values', () => {
        expect(form).toBeTruthy();
        expect(form.get('authType')?.value).toBe(AUTHENTICATION.Unidirectional);
        expect(form.get('subsystemDchapKey')?.value).toBe(null);
      });
    });
  });
});
