import { Component, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from '@angular/core';
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
export class WizardComponent implements OnInit, OnDestroy, OnChanges {
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
    this.initializeSteps();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.stepsTitle && !changes.stepsTitle.isFirstChange()) {
      this.initializeSteps();
    }
  }

  private initializeSteps(): void {
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
