import { Component, OnInit, inject } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { Router } from '@angular/router';
import {
  STEP_TITLES_MIRRORING_CONFIGURED,
  LOCAL_ROLE,
  REMOTE_ROLE
} from './cephfs-mirroring-wizard-step.enum';
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
  steps: Step[] = [];
  title: string = $localize`Create new CephFS Mirroring`;
  description: string = $localize`Configure a new mirroring relationship between clusters`;
  form: FormGroup;

  LOCAL_ROLE = LOCAL_ROLE;
  REMOTE_ROLE = REMOTE_ROLE;

  private wizardStepsService = inject(WizardStepsService);
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
      localRole: [LOCAL_ROLE],
      remoteRole: [null]
    });
  }

  ngOnInit() {
    this.wizardStepsService.setTotalSteps(STEP_TITLES_MIRRORING_CONFIGURED.length);

    const stepsData = this.wizardStepsService.steps$.value;
    this.steps = STEP_TITLES_MIRRORING_CONFIGURED.map((title, index) => ({
      label: title,
      onClick: () => this.goToStep(stepsData[index])
    }));
  }

  goToStep(step: WizardStepModel) {
    if (step) {
      this.wizardStepsService.setCurrentStep(step);
    }
  }

  get sourceChecked(): boolean {
    return this.form.get('localRole')?.value === LOCAL_ROLE;
  }

  onLocalRoleChange() {
    this.form.patchValue({ localRole: LOCAL_ROLE, remoteRole: null });
  }

  onRemoteRoleChange() {
    this.form.patchValue({ localRole: null, remoteRole: REMOTE_ROLE });
  }

  onSubmit() {}

  onCancel() {
    this.router.navigate(['/cephfs/mirroring']);
  }
}
