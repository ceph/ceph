import { Component, OnDestroy } from '@angular/core';
import { Router } from '@angular/router';

import { forkJoin, Subscription } from 'rxjs';
import { finalize } from 'rxjs/operators';

import { ClusterService } from '~/app/shared/api/cluster.service';
import { HostService } from '~/app/shared/api/host.service';
import { ActionLabelsI18n, AppConstants } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Permissions } from '~/app/shared/models/permissions';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';

@Component({
  selector: 'cd-create-cluster',
  templateUrl: './create-cluster.component.html',
  styleUrls: ['./create-cluster.component.scss']
})
export class CreateClusterComponent implements OnDestroy {
  currentStep: WizardStepModel;
  currentStepSub: Subscription;
  permissions: Permissions;
  projectConstants: typeof AppConstants = AppConstants;
  hosts: Array<object> = [];
  stepTitles = ['Add Hosts', 'Review'];
  startClusterCreation = false;
  observables: any = [];

  constructor(
    private authStorageService: AuthStorageService,
    private stepsService: WizardStepsService,
    private router: Router,
    private hostService: HostService,
    private notificationService: NotificationService,
    private actionLabels: ActionLabelsI18n,
    private clusterService: ClusterService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.currentStepSub = this.stepsService.getCurrentStep().subscribe((step: WizardStepModel) => {
      this.currentStep = step;
    });
    this.currentStep.stepIndex = 1;
  }

  createCluster() {
    this.startClusterCreation = true;
  }

  skipClusterCreation() {
    this.clusterService.updateStatus('POST_INSTALLED').subscribe(() => {
      this.notificationService.show(
        NotificationType.info,
        $localize`Cluster creation skipped by user`
      );
      this.router.navigate(['/dashboard']);
    });
  }

  onSubmit() {
    forkJoin(this.observables)
      .pipe(
        finalize(() =>
          this.clusterService.updateStatus('POST_INSTALLED').subscribe(() => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Cluster creation was successful`
            );
            this.router.navigate(['/dashboard']);
          })
        )
      )
      .subscribe({
        error: (error) => error.preventDefault()
      });
  }

  onNextStep() {
    if (!this.stepsService.isLastStep()) {
      this.hostService.list().subscribe((hosts) => {
        hosts.forEach((host) => {
          if (host['status'] === 'maintenance') {
            this.observables.push(this.hostService.update(host['hostname'], false, [], true));
          }
        });
      });
      this.stepsService.moveToNextStep();
    } else {
      this.onSubmit();
    }
  }

  onPreviousStep() {
    if (!this.stepsService.isFirstStep()) {
      this.stepsService.moveToPreviousStep();
    } else {
      this.router.navigate(['/dashboard']);
    }
  }

  showSubmitButtonLabel() {
    return !this.stepsService.isLastStep() ? this.actionLabels.NEXT : $localize`Create Cluster`;
  }

  showCancelButtonLabel() {
    return !this.stepsService.isFirstStep() ? this.actionLabels.BACK : this.actionLabels.CANCEL;
  }

  ngOnDestroy(): void {
    this.currentStepSub.unsubscribe();
  }
}
