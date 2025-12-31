import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { STEP_TITLES_MIRRORING_CONFIGURED } from './cephfs-mirroring-wizard-step.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';
import { BehaviorSubject } from 'rxjs';

@Component({
  selector: 'cd-cephfs-mirroring-wizard',
  templateUrl: './cephfs-mirroring-wizard.component.html',
  standalone: false,
  styleUrls: ['./cephfs-mirroring-wizard.component.scss']
})
export class CephfsMirroringWizardComponent implements OnInit {
  stepTitles: Step[] = [];
  currentStepIndex: number = 0;
  currentStep: WizardStepModel;
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
  // onNextStep() {
  //   if (!this.wizardStepsService.isLastStep()) {
  //     this.wizardStepsService.moveToNextStep();
  //   } else {
  //     this.onSubmit();
  //   }
  // }

  // onNextStep() {
  //   // you can validate before moving next
  //   if (this.currentStepIndex === 1 && !this.selectedFilesystem$.value) {
  //     return; // block next
  //   }
  //   this.wizardStepsService.moveToNextStep();
  // }

// 4




  onSubmit() {
    throw new Error('Method not implemented.');
  }
}
