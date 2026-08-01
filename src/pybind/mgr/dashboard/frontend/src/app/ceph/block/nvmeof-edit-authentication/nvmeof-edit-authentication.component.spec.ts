import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { of, throwError } from 'rxjs';

import { GridModule, InputModule, RadioModule, TagModule } from 'carbon-components-angular';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { AUTHENTICATION } from '~/app/shared/models/nvmeof';
import {
  NvmeofEditAuthenticationComponent,
  SUBSYSTEM_NQN_TOKEN,
  GROUP_NAME_TOKEN
} from './nvmeof-edit-authentication.component';
import { NvmeofSubsystemsStepThreeComponent } from '../nvmeof-subsystems-form/nvmeof-subsystem-step-3/nvmeof-subsystem-step-3.component';

describe('NvmeofEditAuthenticationComponent', () => {
  let component: NvmeofEditAuthenticationComponent;
  let fixture: ComponentFixture<NvmeofEditAuthenticationComponent>;
  let nvmeofService: jest.Mocked<
    Pick<NvmeofService, 'getInitiators' | 'getSubsystem' | 'updateAuthenticationKey'>
  >;
  let notificationService: { show: jest.Mock };
  let modalService: { dismissAll: jest.Mock };

  const mockSubsystemNQN = 'nqn.2014-08.org.nvmexpress:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6';
  const mockGroupName = 'default';
  const mockHosts = [
    { nqn: 'nqn.2014-08.org.nvmexpress:uuid:host-1', use_dhchap: false },
    { nqn: 'nqn.2014-08.org.nvmexpress:uuid:host-2', use_dhchap: false }
  ];
  const mockHostsWithKey = [{ nqn: 'nqn.2014-08.org.nvmexpress:uuid:host-1', use_dhchap: true }];

  beforeEach(waitForAsync(() => {
    nvmeofService = {
      getInitiators: jest.fn().mockReturnValue(of([])),
      getSubsystem: jest.fn().mockReturnValue(of({ has_dhchap_key: false })),
      updateAuthenticationKey: jest.fn().mockReturnValue(of(undefined))
    };
    notificationService = { show: jest.fn() };
    modalService = { dismissAll: jest.fn() };

    TestBed.configureTestingModule({
      declarations: [NvmeofEditAuthenticationComponent, NvmeofSubsystemsStepThreeComponent],
      imports: [
        ReactiveFormsModule,
        HttpClientTestingModule,
        RouterTestingModule,
        SharedModule,
        GridModule,
        InputModule,
        RadioModule,
        TagModule
      ],
      providers: [
        { provide: NvmeofService, useValue: nvmeofService },
        { provide: NotificationService, useValue: notificationService },
        { provide: ModalCdsService, useValue: modalService },
        { provide: SUBSYSTEM_NQN_TOKEN, useValue: mockSubsystemNQN },
        { provide: GROUP_NAME_TOKEN, useValue: mockGroupName }
      ]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofEditAuthenticationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges(); // runs ngOnInit + ngAfterViewInit
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should inject subsystemNQN and groupName', () => {
    expect(component.subsystemNQN).toBe(mockSubsystemNQN);
    expect(component.groupName).toBe(mockGroupName);
  });

  it('should fetch initiators on init and build stepTwoValue with host NQNs', () => {
    nvmeofService.getInitiators.mockReturnValue(of(mockHosts));
    component.ngOnInit();
    expect(nvmeofService.getInitiators).toHaveBeenCalledWith(mockSubsystemNQN, mockGroupName);
    expect(component.stepTwoValue?.addedHosts).toEqual([
      'nqn.2014-08.org.nvmexpress:uuid:host-1',
      'nqn.2014-08.org.nvmexpress:uuid:host-2'
    ]);
  });

  it('should handle hosts returned as { hosts: [...] } response shape', () => {
    nvmeofService.getInitiators.mockReturnValue(of({ hosts: mockHosts }));
    component.ngOnInit();
    expect(component.stepTwoValue?.addedHosts.length).toBe(2);
  });

  it('should handle empty host list', () => {
    nvmeofService.getInitiators.mockReturnValue(of([]));
    component.ngOnInit();
    expect(component.stepTwoValue?.addedHosts).toEqual([]);
  });

  describe('initialAuthType pre-selection', () => {
    it('should default to Unidirectional when subsystem has no DHCHAP key', () => {
      expect(component.initialAuthType).toBe(AUTHENTICATION.Unidirectional);
      expect(component.authStep.formGroup.get('authType')?.value).toBe(
        AUTHENTICATION.Unidirectional
      );
    });

    it('should patch form control to Bidirectional when subsystem and host both have keys', () => {
      nvmeofService.getSubsystem.mockReturnValue(of({ has_dhchap_key: true }));
      nvmeofService.getInitiators.mockReturnValue(of(mockHostsWithKey));
      component.ngOnInit();
      expect(component.initialAuthType).toBe(AUTHENTICATION.Bidirectional);
      component.authStep.initialAuthType = component.initialAuthType;
      expect(component.authStep.formGroup.get('authType')?.value).toBe(
        AUTHENTICATION.Bidirectional
      );
    });

    it('should set Unidirectional when only host has a key', () => {
      nvmeofService.getSubsystem.mockReturnValue(of({ has_dhchap_key: false }));
      nvmeofService.getInitiators.mockReturnValue(of(mockHostsWithKey));
      component.ngOnInit();
      expect(component.initialAuthType).toBe(AUTHENTICATION.Unidirectional);
      expect(component.authStep.formGroup.get('authType')?.value).toBe(
        AUTHENTICATION.Unidirectional
      );
    });

    it('should set Unidirectional when no keys are present', () => {
      nvmeofService.getSubsystem.mockReturnValue(of({ has_dhchap_key: false }));
      nvmeofService.getInitiators.mockReturnValue(of([]));
      component.ngOnInit();
      expect(component.initialAuthType).toBe(AUTHENTICATION.Unidirectional);
    });
  });

  describe('showAuthAlert', () => {
    it('should be false by default', () => {
      expect(component.showAuthAlert).toBe(false);
    });

    it('should remain false when subsystem has no DHCHAP key and radio changes to Unidirectional', () => {
      nvmeofService.getSubsystem.mockReturnValue(of({ has_dhchap_key: false }));
      component.ngOnInit();
      component.authStep.formGroup.get('authType')?.setValue(AUTHENTICATION.Unidirectional);
      expect(component.showAuthAlert).toBe(false);
    });

    it('should remain false when subsystem has DHCHAP key and radio changes to Bidirectional', () => {
      nvmeofService.getSubsystem.mockReturnValue(of({ has_dhchap_key: true }));
      component.ngOnInit();
      component.authStep.formGroup.get('authType')?.setValue(AUTHENTICATION.Bidirectional);
      expect(component.showAuthAlert).toBe(false);
    });

    it('should become true when subsystem has DHCHAP key and radio changes to Unidirectional', () => {
      nvmeofService.getSubsystem.mockReturnValue(of({ has_dhchap_key: true }));
      component.ngOnInit();
      component.authStep.formGroup.get('authType')?.setValue(AUTHENTICATION.Unidirectional);
      expect(component.showAuthAlert).toBe(true);
    });
  });

  describe('onSubmit', () => {
    it('should not call updateAuthenticationKey when form is invalid', () => {
      const form = component.authStep?.formGroup;
      if (form) {
        form.markAllAsTouched();
        form.setErrors({ test: true });
      }
      component.onSubmit();
      expect(nvmeofService.updateAuthenticationKey).not.toHaveBeenCalled();
    });

    it('should not call updateAuthenticationKey when authStep is absent', () => {
      component.authStep = undefined as any;
      component.onSubmit();
      expect(nvmeofService.updateAuthenticationKey).not.toHaveBeenCalled();
    });

    it('should extract and submit authentication settings on valid Bidirectional form', () => {
      const validKey = 'Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY=';
      const form = component.authStep.formGroup;
      form.get('authType')?.setValue(AUTHENTICATION.Bidirectional);
      form.get('subsystemDchapKey')?.setValue(validKey);
      component.onSubmit();
      expect(nvmeofService.updateAuthenticationKey).toHaveBeenCalledWith(
        mockSubsystemNQN,
        mockGroupName,
        expect.objectContaining({
          authType: AUTHENTICATION.Bidirectional,
          subsystemKey: validKey
        })
      );
    });

    it('should submit Unidirectional authentication without subsystem key', () => {
      const form = component.authStep?.formGroup;
      form?.get('authType')?.setValue(AUTHENTICATION.Unidirectional);
      component.onSubmit();
      expect(nvmeofService.updateAuthenticationKey).toHaveBeenCalledWith(
        mockSubsystemNQN,
        mockGroupName,
        expect.objectContaining({ authType: AUTHENTICATION.Unidirectional })
      );
    });

    it('should show notification and emit closeChange on successful submit', (done) => {
      // closeChange fires synchronously inside onSubmit's complete handler,
      // but dismissAll is called after — use setTimeout to assert the full chain.
      component.closeChange.subscribe(() => {
        setTimeout(() => {
          expect(notificationService.show).toHaveBeenCalledWith(
            expect.anything(),
            expect.stringContaining('updated'),
            expect.anything()
          );
          expect(modalService.dismissAll).toHaveBeenCalled();
          done();
        }, 0);
      });

      const form = component.authStep?.formGroup;
      form?.get('authType')?.setValue(AUTHENTICATION.Unidirectional);
      component.onSubmit();
    });

    it('should mark form with submission error on API failure', (done) => {
      const form = component.authStep?.formGroup;
      form?.get('authType')?.setValue(AUTHENTICATION.Unidirectional);
      nvmeofService.updateAuthenticationKey.mockReturnValue(
        throwError(() => ({ status: 500, message: 'Server error' }))
      );

      component.onSubmit();

      setTimeout(() => {
        expect(component.isSubmitLoading).toBe(false);
        expect(form?.hasError('cdSubmitButton')).toBe(true);
        expect(notificationService.show).not.toHaveBeenCalled();
        expect(modalService.dismissAll).not.toHaveBeenCalled();
        done();
      }, 0);
    });

    it('should clear isSubmitLoading after error', (done) => {
      const form = component.authStep?.formGroup;
      form?.get('authType')?.setValue(AUTHENTICATION.Unidirectional);
      nvmeofService.updateAuthenticationKey.mockReturnValue(
        throwError(() => new Error('Network error'))
      );

      component.onSubmit();

      setTimeout(() => {
        expect(component.isSubmitLoading).toBe(false);
        done();
      }, 0);
    });
  });
});
