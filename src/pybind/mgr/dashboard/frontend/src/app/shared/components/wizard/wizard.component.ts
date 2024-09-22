import { Component, Input, OnDestroy, OnInit } from '@angular/core';
import { Step } from 'carbon-components-angular';

import * as _ from 'lodash';
import { Observable, Subscription } from 'rxjs';

import { WizardStepModel } from '~/app/shared/models/wizard-steps';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';

@Component({
  selector: 'cd-wizard',
  templateUrl: './wizard.component.html',
  styleUrls: ['./wizard.component.scss']
})
export class WizardComponent implements OnInit, OnDestroy {
  @Input()
  stepsTitle: Step[];

  steps: Observable<WizardStepModel[]>;
  currentStep: WizardStepModel;
  currentStepSub: Subscription;

  constructor(private stepsService: WizardStepsService) {
    this.stepsTitle?.forEach((steps, index) => {
      steps.onClick = () => (this.currentStep.stepIndex = index);
    });
  }

  ngOnInit(): void {
    this.stepsService.setTotalSteps(this.stepsTitle.length);
    this.steps = this.stepsService.getSteps();
    this.currentStepSub = this.stepsService.getCurrentStep().subscribe((step: WizardStepModel) => {
      this.currentStep = step;
    });
  }

  onStepClick(step: WizardStepModel) {
    this.stepsService.setCurrentStep(step);
  }

  ngOnDestroy(): void {
    this.currentStepSub.unsubscribe();
  }
}
