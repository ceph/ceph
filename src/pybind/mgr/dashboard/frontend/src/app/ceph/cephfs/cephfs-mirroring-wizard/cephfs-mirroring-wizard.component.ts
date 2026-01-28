import { Component, OnInit, ChangeDetectorRef, inject } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { Router } from '@angular/router';
import { BehaviorSubject } from 'rxjs';
import { STEP_TITLES_MIRRORING_CONFIGURED } from './cephfs-mirroring-wizard-step.enum';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';
import { FormBuilder, FormGroup } from '@angular/forms';

@Component({
  selector: 'cd-cephfs-mirroring-wizard',
  templateUrl: './cephfs-mirroring-wizard.component.html',
  standalone: false,
  styleUrls: ['./cephfs-mirroring-wizard.component.scss']
})
export class CephfsMirroringWizardComponent implements OnInit {
  stepTitles: Step[] = [];
  currentStep: WizardStepModel | null = null;
  currentStepIndex: number = 0;
  selectedRole: string = 'source';
  steps: any;
  lastStep: number = null;
  isWizardOpen: boolean = true;
    selectedFilesystem$: BehaviorSubject<any> = new BehaviorSubject<any>(null);
  selectedEntity$: BehaviorSubject<any> = new BehaviorSubject<any>(null);
  title: string = $localize`Create New CephFS Mirroring`;
  description: string = $localize`Configure a new mirroring relationship between clusters`;
  form: FormGroup;

  sourceChecked: boolean = false;
  targetChecked: boolean = false;

  private wizardStepsService = inject(WizardStepsService);
  private cdr = inject(ChangeDetectorRef);
  private fb = inject(FormBuilder);
  private router = inject(Router);

  sourceList: string[] = [
    $localize`Sends data to remote clusters`,
    $localize`Requires bootstrap token from target`,
    $localize`Manages snapshot schedules`
  ];

  targetList: string[] = [
    $localize`Receives data from source clusters`,
    $localize`Generates bootstrap token`,
    $localize`Stores replicated snapshots`
  ];

  constructor() {
    this.form = this.fb.group({
      clusterRole1: [],
      clusterRole2: []
    });
  }

  ngOnInit() {
    if (!this.steps) {
      this.steps = [];
    }

    this.stepTitles = STEP_TITLES_MIRRORING_CONFIGURED.map((title, index) => ({
      label: title,
      onClick: () => this.goToStep(index)
    }));

    if (!this.steps || this.steps.length === 0) {
      this.steps = this.initializeSteps();
    }

    this.lastStep = this.steps.length - 1;

    this.wizardStepsService.setTotalSteps(this.stepTitles.length);

    this.wizardStepsService.getCurrentStep().subscribe((step) => {
      this.currentStep = step;
      this.currentStepIndex = step?.stepIndex || 0;
      this.cdr.detectChanges();
    });

    // Initialize radio buttons (default to source)
    this.sourceChecked = true;
    this.targetChecked = false;
    this.form.patchValue({ clusterRole1: 'source', clusterRole2: null });
  }

  initializeSteps(): WizardStepModel[] {
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
    this.currentStep = this.steps[event.index];
  }

  onRoleChange() {
    this.sourceChecked = true;
    this.targetChecked = false;
    this.form.patchValue({ clusterRole1: 'source', clusterRole2: null });
  }

  onRole2Change() {
    this.sourceChecked = false;
    this.targetChecked = true;
    this.form.patchValue({ clusterRole1: null, clusterRole2: 'target' });
  }

  onSubmit() {}

  onCancel() {
    this.router.navigate(['/cephfs/mirroring']);
  }
}
