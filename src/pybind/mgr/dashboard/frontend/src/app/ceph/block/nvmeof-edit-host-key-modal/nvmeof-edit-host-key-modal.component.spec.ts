import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { By } from '@angular/platform-browser';
import { ToastrModule } from 'ngx-toastr';
import { of, throwError } from 'rxjs';

import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofEditHostKeyModalComponent } from './nvmeof-edit-host-key-modal.component';

describe('NvmeofEditHostKeyModalComponent', () => {
  let component: NvmeofEditHostKeyModalComponent;
  let fixture: ComponentFixture<NvmeofEditHostKeyModalComponent>;
  let nvmeofService: NvmeofService;
  let taskWrapperService: TaskWrapperService;

  const mockSubsystemNQN = 'nqn.2014-08.org.nvmexpress:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6';
  const mockHostNQN = 'nqn.2014-08.org.nvmexpress:uuid:12345678-1234-1234-1234-1234567890ab';
  const mockGroup = 'default';

  const nvmeofServiceSpy = {
    updateHostKey: jasmine.createSpy('updateHostKey').and.returnValue(of(null))
  };

  const taskWrapperServiceSpy = {
    wrapTaskAroundCall: jasmine.createSpy('wrapTaskAroundCall').and.callFake(({ call }) => call)
  };

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [NvmeofEditHostKeyModalComponent],
        imports: [
          ReactiveFormsModule,
          HttpClientTestingModule,
          RouterTestingModule,
          SharedModule,
          ToastrModule.forRoot()
        ],
        providers: [
          { provide: NvmeofService, useValue: nvmeofServiceSpy },
          { provide: TaskWrapperService, useValue: taskWrapperServiceSpy },
          { provide: 'subsystemNQN', useValue: mockSubsystemNQN },
          { provide: 'hostNQN', useValue: mockHostNQN },
          { provide: 'group', useValue: mockGroup },
          { provide: 'dhchapKey', useValue: '' }
        ]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofEditHostKeyModalComponent);
    component = fixture.componentInstance;
    nvmeofService = TestBed.inject(NvmeofService);
    taskWrapperService = TestBed.inject(TaskWrapperService);
    nvmeofServiceSpy.updateHostKey.calls.reset();
    taskWrapperServiceSpy.wrapTaskAroundCall.calls.reset();
    nvmeofServiceSpy.updateHostKey.and.returnValue(of(null));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize the form with empty dhchapKey', () => {
    expect(component.editForm).toBeDefined();
    expect(component.editForm.get('dhchapKey').value).toBe('');
    expect(component.editForm.valid).toBe(false);
  });

  it('should inject subsystemNQN, hostNQN and group', () => {
    expect(component.subsystemNQN).toBe(mockSubsystemNQN);
    expect(component.hostNQN).toBe(mockHostNQN);
    expect(component.group).toBe(mockGroup);
  });

  it('should display the host NQN in the form label', () => {
    const label = fixture.debugElement.query(By.css('cds-text-label'));
    expect(label.nativeElement.textContent).toContain(mockHostNQN);
  });

  it('should not submit if form is invalid', () => {
    component.onSubmit();
    expect(nvmeofService.updateHostKey).not.toHaveBeenCalled();
  });

  it('should submit successfully when form is valid', () => {
    const mockKey = 'Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY=';
    component.editForm.patchValue({ dhchapKey: mockKey });
    expect(component.editForm.valid).toBe(true);

    spyOn(component, 'closeModal');
    component.onSubmit();

    expect(nvmeofService.updateHostKey).toHaveBeenCalledWith(mockSubsystemNQN, {
      host_nqn: mockHostNQN,
      dhchap_key: mockKey,
      gw_group: mockGroup
    });
    expect(taskWrapperService.wrapTaskAroundCall).toHaveBeenCalled();
    expect(component.closeModal).toHaveBeenCalled();
  });

  it('should call updateHostKey with correct task metadata on submit', () => {
    const mockKey = 'Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY=';
    component.editForm.patchValue({ dhchapKey: mockKey });
    spyOn(component, 'closeModal');

    component.onSubmit();

    const callArgs = taskWrapperServiceSpy.wrapTaskAroundCall.calls.mostRecent().args[0];
    expect(callArgs.task.name).toBe('nvmeof/initiator/edit');
    expect(callArgs.task.metadata).toEqual({
      nqn: mockSubsystemNQN
    });
  });

  it('should handle error during submission', () => {
    const mockKey = 'Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY=';
    component.editForm.patchValue({ dhchapKey: mockKey });
    spyOn(component.editForm, 'setErrors');

    nvmeofServiceSpy.updateHostKey.and.returnValue(throwError(() => ({ status: 500 })));

    component.onSubmit();

    expect(nvmeofService.updateHostKey).toHaveBeenCalled();
    expect(component.editForm.setErrors).toHaveBeenCalledTimes(1);
    expect(component.editForm.setErrors).toHaveBeenCalledWith({ cdSubmitButton: true });
  });

  it('should not close modal on error', () => {
    const mockKey = 'Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY=';
    component.editForm.patchValue({ dhchapKey: mockKey });
    spyOn(component, 'closeModal');

    nvmeofServiceSpy.updateHostKey.and.returnValue(throwError(() => ({ status: 400 })));

    component.onSubmit();

    expect(component.closeModal).not.toHaveBeenCalled();
  });

  it('should be valid with 43-character unpadded Base64 key with DHHC-1: prefix and trailing colon', () => {
    const prefixedSuffixedKey = 'DHHC-1:00:Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY:';
    component.editForm.patchValue({ dhchapKey: prefixedSuffixedKey });
    expect(component.editForm.get('dhchapKey').valid).toBe(true);
    expect(component.editForm.valid).toBe(true);
  });

  describe('Save button click', () => {
    it('should trigger onSubmit and call updateHostKey when Save is clicked with valid form', () => {
      const mockKey = 'Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY=';
      component.editForm.patchValue({ dhchapKey: mockKey });
      fixture.detectChanges();

      spyOn(component, 'closeModal');

      // Step 1: Save (Confirmation)
      fixture.debugElement
        .query(By.css('cd-form-button-panel'))
        .triggerEventHandler('submitActionEvent', null);
      fixture.detectChanges();
      expect(component.showConfirmation).toBe(true);

      // Step 2: Save changes (Actual submit)
      fixture.debugElement
        .query(By.css('cd-form-button-panel'))
        .triggerEventHandler('submitActionEvent', null);
      fixture.detectChanges();

      expect(nvmeofService.updateHostKey).toHaveBeenCalledWith(mockSubsystemNQN, {
        host_nqn: mockHostNQN,
        dhchap_key: mockKey,
        gw_group: mockGroup
      });
      expect(taskWrapperService.wrapTaskAroundCall).toHaveBeenCalled();
      expect(component.closeModal).toHaveBeenCalled();
    });

    it('should not call updateHostKey when Save is clicked with empty form', () => {
      fixture.detectChanges();

      const submitPanel = fixture.debugElement.query(By.css('cd-form-button-panel'));
      submitPanel.triggerEventHandler('submitActionEvent', null);
      fixture.detectChanges();

      expect(nvmeofService.updateHostKey).not.toHaveBeenCalled();
    });

    it('should call closeModal when Cancel is clicked', () => {
      spyOn(component, 'closeModal');
      fixture.detectChanges();

      const submitPanel = fixture.debugElement.query(By.css('cd-form-button-panel'));
      submitPanel.triggerEventHandler('backActionEvent', null);
      fixture.detectChanges();

      expect(component.closeModal).toHaveBeenCalled();
    });
  });
});
