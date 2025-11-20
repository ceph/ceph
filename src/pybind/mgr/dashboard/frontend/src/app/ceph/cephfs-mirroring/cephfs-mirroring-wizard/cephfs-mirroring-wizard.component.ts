import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { STEP_TITLES_MIRRORING_CONFIGURED } from './cephfs-mirroring-wizard-step.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';

@Component({
  selector: 'cd-cephfs-mirroring-wizard',
  templateUrl: './cephfs-mirroring-wizard.component.html',
  styleUrls: ['./cephfs-mirroring-wizard.component.scss']
})
export class CephfsMirroringWizardComponent implements OnInit {
  stepTitles: Step[] = [];
  currentStepIndex: number = 0;
  currentStep: WizardStepModel;
  selectedRole: string = 'source';
  icons = Icons;

  sourceList: string[] = [
    'Sends data to remote clusters',
    'Requires bootstrap token from target',
    'Manages snapshot schedules'
  ];

  targetList: string[] = [
    'Receives data from source clusters',
    'Generates bootstrap token',
    'Stores replicated snapshots'
  ];

  constructor(private wizardStepsService: WizardStepsService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.stepTitles = STEP_TITLES_MIRRORING_CONFIGURED.map((title, index) => ({
      label: title,
      onClick: () => this.goToStep(index)
    }));

    this.wizardStepsService.setTotalSteps(this.stepTitles.length);

    this.wizardStepsService.getCurrentStep().subscribe((step) => {
      this.currentStep = step;
      this.currentStepIndex = step?.stepIndex || 0;
      this.cdr.detectChanges();
    });
  }

  goToStep(index: number) {
    this.wizardStepsService
      .getSteps()
      .subscribe((steps) => {
        const step = steps[index];
        if (step) {
          this.wizardStepsService.setCurrentStep(step);
        }
      })
      .unsubscribe();
  }

  onNextStep() {
    if (!this.wizardStepsService.isLastStep()) {
      this.wizardStepsService.moveToNextStep();
    } else {
      this.onSubmit();
    }
  }

  onPreviousStep() {
    if (!this.wizardStepsService.isFirstStep()) {
      this.wizardStepsService.moveToPreviousStep();
    }
  }

  onSubmit() {}

  selectRole(role: string) {
    this.selectedRole = role;
  }

  getCurrentRoleList(): string[] {
    return this.selectedRole === 'source' ? this.sourceList : this.targetList;
  }
}
