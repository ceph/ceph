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

describe('CephfsMirroringWizardComponent', () => {
  let component: CephfsMirroringWizardComponent;
  let fixture: ComponentFixture<CephfsMirroringWizardComponent>;
  let wizardStepsService: jest.Mocked<WizardStepsService>;
  let router: jest.Mocked<Router>;

  const mockSteps: WizardStepModel[] = [{ id: 1 } as WizardStepModel, { id: 2 } as WizardStepModel];

  beforeEach(async () => {
    wizardStepsService = ({
      setTotalSteps: jest.fn(),
      setCurrentStep: jest.fn(),
      steps$: new BehaviorSubject<WizardStepModel[]>(mockSteps)
    } as unknown) as jest.Mocked<WizardStepsService>;

    router = ({
      navigate: jest.fn()
    } as unknown) as jest.Mocked<Router>;

    await TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, RadioModule],
      declarations: [CephfsMirroringWizardComponent],
      providers: [
        FormBuilder,
        { provide: WizardStepsService, useValue: wizardStepsService },
        { provide: Router, useValue: router }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

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

  it('should return true for sourceChecked when local role is selected', () => {
    component.form.patchValue({ localRole: LOCAL_ROLE });
    expect(component.sourceChecked).toBe(true);
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
