import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { STEP_TITLES_MIRRORING_CONFIGURED } from './cephfs-mirroring-wizard-step.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { BehaviorSubject } from 'rxjs';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';

@Component({
  selector: 'cd-cephfs-mirroring-wizard',
  templateUrl: './cephfs-mirroring-wizard.component.html',
  standalone: false,
  styleUrls: ['./cephfs-mirroring-wizard.component.scss']
})
export class CephfsMirroringWizardComponent implements OnInit {
  stepTitles: Step[] = [];
  currentStep: WizardStepModel | null = null;  // Change to WizardStepModel
  currentStepIndex: number = 0;
  selectedRole: string = 'source';
  icons = Icons;
  selectedFilesystem$: BehaviorSubject<any> = new BehaviorSubject<any>(null);

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

  steps: any;

  lastStep: number = null;
  isWizardOpen: boolean = true;

  constructor(private wizardStepsService: WizardStepsService, private cdr: ChangeDetectorRef) {}

ngOnInit() {
  // Initialize steps if not already populated
  if (!this.steps) {
    this.steps = []; // Initialize it as an empty array or populate it based on the logic
  }

  this.stepTitles = STEP_TITLES_MIRRORING_CONFIGURED.map((title, index) => ({
    label: title,
    onClick: () => this.goToStep(index)
  }));

  // Ensure that steps are populated correctly
  if (!this.steps || this.steps.length === 0) {
    this.steps = this.initializeSteps(); // Call a method to populate steps or fetch data from service
  }

  this.lastStep = this.steps.length - 1; // Update the lastStep based on the steps length

  this.wizardStepsService.setTotalSteps(this.stepTitles.length);

  this.wizardStepsService.getCurrentStep().subscribe((step) => {
    // Set the currentStep properly
    this.currentStep = step;
    this.currentStepIndex = step?.stepIndex || 0;
    this.cdr.detectChanges();
  });
}

// Example of how you can initialize steps
initializeSteps(): WizardStepModel[] {
  // You can return a predefined list or fetch from a service
  return [
    { stepIndex: 0 } as WizardStepModel,
    { stepIndex: 1 } as WizardStepModel,
    { stepIndex: 2 } as WizardStepModel
  ];
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

 

  onStepSelect(event: { step: Step; index: number }) {
  // Instead of assigning a number, find the corresponding step model
  this.currentStep = this.steps[event.index];  // Set the full step model instead of just the index
}


  // You may need to adjust onPrevious and onNext methods to handle WizardStepModel as well
  onPrevious() {
    if (this.currentStep && this.currentStep.stepIndex !== 0) {
      this.currentStep = { ...this.currentStep, stepIndex: this.currentStep.stepIndex - 1 };  // Adjust stepIndex if needed
    }
  }

  onNext() {
    if (this.currentStep && !this.steps[this.currentStep.stepIndex].invalid) {
      this.currentStep = { ...this.currentStep, stepIndex: this.currentStep.stepIndex + 1 };  // Adjust stepIndex if needed
    }
  }

    updateSelectedFilesystem(filesystem: any) {
    this.selectedFilesystem$.next(filesystem);
  }


  closeWizard() {
  this.isWizardOpen = false;
}
}

