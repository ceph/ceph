import { ComponentFixture, TestBed } from '@angular/core/testing';
import { CephfsMirroringWizardComponent } from './cephfs-mirroring-wizard.component';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { FormBuilder, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { BehaviorSubject } from 'rxjs';
import {
  STEP_TITLES_MIRRORING_CONFIGURED,
  LOCAL_ROLE,
  REMOTE_ROLE
} from './cephfs-mirroring-wizard-step.enum';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { RadioModule } from 'carbon-components-angular';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CephfsMirroringWizardComponent', () => {
  const mockSteps: WizardStepModel[] = [
    { stepIndex: 0, isComplete: false },
    { stepIndex: 1, isComplete: false }
  ];
  let component: CephfsMirroringWizardComponent;
  let fixture: ComponentFixture<CephfsMirroringWizardComponent>;
  let wizardStepsService: jest.Mocked<WizardStepsService> = {
    setTotalSteps: jest.fn(),
    setCurrentStep: jest.fn(),
    steps$: new BehaviorSubject<WizardStepModel[]>(mockSteps)
  } as unknown as jest.Mocked<WizardStepsService>;
  let router: jest.Mocked<Router> = {
    navigate: jest.fn()
  } as unknown as jest.Mocked<Router>;

  configureTestBed({
    imports: [ReactiveFormsModule, RadioModule],
    declarations: [CephfsMirroringWizardComponent],
    providers: [
      FormBuilder,
      { provide: WizardStepsService, useFactory: () => wizardStepsService },
      { provide: Router, useFactory: () => router }
    ],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsMirroringWizardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize wizard steps on ngOnInit', () => {
    expect(wizardStepsService.setTotalSteps).toHaveBeenCalledWith(
      STEP_TITLES_MIRRORING_CONFIGURED.length
    );

    expect(component.steps.length).toBe(STEP_TITLES_MIRRORING_CONFIGURED.length);
  });

  it('should navigate to step when goToStep is called', () => {
    component.goToStep(mockSteps[0]);

    expect(wizardStepsService.setCurrentStep).toHaveBeenCalledWith(mockSteps[0]);
  });

  it('should initialize form with local role selected', () => {
    expect(component.form.value).toEqual({
      localRole: LOCAL_ROLE,
      remoteRole: null
    });
  });

  it('should update form on local role change', () => {
    component.onLocalRoleChange();

    expect(component.form.value).toEqual({
      localRole: LOCAL_ROLE,
      remoteRole: null
    });
  });

  it('should update form on remote role change', () => {
    component.onRemoteRoleChange();

    expect(component.form.value).toEqual({
      localRole: null,
      remoteRole: REMOTE_ROLE
    });
  });

  it('should navigate to mirroring list on cancel', () => {
    component.onCancel();
    expect(router.navigate).toHaveBeenCalledWith(['/cephfs/mirroring']);
  });
});
