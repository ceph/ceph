import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystemsStepTwoComponent } from './nvmeof-subsystem-step-2.component';
import { GridModule, InputModule, RadioModule, TagModule } from 'carbon-components-angular';

describe('NvmeofSubsystemsStepTwoComponent', () => {
  let component: NvmeofSubsystemsStepTwoComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsStepTwoComponent>;
  let form: CdFormGroup;
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
        InputModule,
        GridModule,
        RadioModule,
        TagModule,
        ToastrModule.forRoot()
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSubsystemsStepTwoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    form = component.formGroup;
    component.group = mockGroupName;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('form initialization', () => {
    it('should initialize form with default values', () => {
      expect(form).toBeTruthy();
      expect(form.get('hostType')?.value).toBe(component.HOST_TYPE.SPECIFIC);
      expect(form.get('hostname')?.value).toBe('');
      expect(form.get('addedHosts')?.value).toEqual([]);
    });
  });

  describe('showRightInfluencer', () => {
    it('should return true when hostType is SPECIFIC', () => {
      form.get('hostType')?.setValue(component.HOST_TYPE.SPECIFIC);
      expect(component.showRightInfluencer()).toBeTruthy();
    });

    it('should return false when hostType is ALL', () => {
      form.get('hostType')!.setValue(component.HOST_TYPE.ALL);

      expect(form.get('hostType')!.value).toBe(component.HOST_TYPE.ALL);
      expect(component.showRightInfluencer()).toBeFalsy();
    });
  });

  describe('hostname validation', () => {
    it('should not require hostname when hostType is ALL', () => {
      form.get('hostType')?.setValue(component.HOST_TYPE.ALL);
      form.get('hostname')?.setValue('');

      expect(form.get('hostname')?.hasError('required')).toBeFalsy();
    });
  });

  describe('custom NQN validator', () => {
    it('should mark invalid NQN format', () => {
      form.get('hostname')?.setValue('invalid-nqn');

      expect(form.get('hostname')?.hasError('pattern')).toBeTruthy();
    });

    it('should accept valid NQN format', () => {
      const validNqn = 'nqn.2023-01.com.example:host1';
      form.get('hostname')?.setValue(validNqn);

      expect(form.get('hostname')?.valid).toBeTruthy();
    });
  });

  describe('addHost', () => {
    it('should add hostname to addedHosts list', () => {
      const hostname = 'nqn.2023-01.com.example:host1';
      form.get('hostname')?.setValue(hostname);

      component.addHost();

      expect(form.get('addedHosts')?.value).toEqual([hostname]);
      expect(component.addedHostsLength).toBe(1);
    });

    it('should not add empty hostname', () => {
      form.get('hostname')?.setValue('');

      component.addHost();

      expect(form.get('addedHosts')?.value).toEqual([]);
      expect(component.addedHostsLength).toBe(0);
    });
  });
});
