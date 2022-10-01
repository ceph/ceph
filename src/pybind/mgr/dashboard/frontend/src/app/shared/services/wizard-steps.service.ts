import { Injectable } from '@angular/core';

import { BehaviorSubject, Observable } from 'rxjs';

import { WizardStepModel } from '~/app/shared/models/wizard-steps';

const initialStep = [{ stepIndex: 1, isComplete: false }];

@Injectable({
  providedIn: 'root'
})
export class WizardStepsService {
  steps$: BehaviorSubject<WizardStepModel[]>;
  currentStep$: BehaviorSubject<WizardStepModel> = new BehaviorSubject<WizardStepModel>(null);

  constructor() {
    this.steps$ = new BehaviorSubject<WizardStepModel[]>(initialStep);
    this.currentStep$.next(this.steps$.value[0]);
  }

  setTotalSteps(step: number) {
    const steps: WizardStepModel[] = [];
    for (let i = 1; i <= step; i++) {
      steps.push({ stepIndex: i, isComplete: false });
    }
    this.steps$ = new BehaviorSubject<WizardStepModel[]>(steps);
  }

  setCurrentStep(step: WizardStepModel): void {
    this.currentStep$.next(step);
  }

  getCurrentStep(): Observable<WizardStepModel> {
    return this.currentStep$.asObservable();
  }

  getSteps(): Observable<WizardStepModel[]> {
    return this.steps$.asObservable();
  }

  moveToNextStep(): void {
    const index = this.currentStep$.value.stepIndex;
    this.currentStep$.next(this.steps$.value[index]);
  }

  moveToPreviousStep(): void {
    const index = this.currentStep$.value.stepIndex - 1;
    this.currentStep$.next(this.steps$.value[index - 1]);
  }

  isLastStep(): boolean {
    return this.currentStep$.value.stepIndex === this.steps$.value.length;
  }

  isFirstStep(): boolean {
    return this.currentStep$.value?.stepIndex - 1 === 0;
  }
}
