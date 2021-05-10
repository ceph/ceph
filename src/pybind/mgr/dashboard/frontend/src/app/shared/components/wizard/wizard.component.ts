import { Component, Input, OnDestroy, OnInit } from '@angular/core';

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
  stepsTitle: string[];

  steps: Observable<WizardStepModel[]>;
  currentSteps: Observable<WizardStepModel>;
  currentStep: WizardStepModel;
  currentStepSub: Subscription;

  constructor(private stepsService: WizardStepsService) {}

  ngOnInit(): void {
    this.stepsService.setTotalSteps(this.stepsTitle.length);
    this.steps = this.stepsService.getSteps();
    this.currentSteps = this.stepsService.getCurrentStep();
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
