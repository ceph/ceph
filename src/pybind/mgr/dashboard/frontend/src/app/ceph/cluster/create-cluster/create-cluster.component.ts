import { Component, OnDestroy, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { forkJoin, Subscription } from 'rxjs';
import { finalize } from 'rxjs/operators';

import { ClusterService } from '~/app/shared/api/cluster.service';
import { HostService } from '~/app/shared/api/host.service';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { ActionLabelsI18n, AppConstants } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Permissions } from '~/app/shared/models/permissions';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';

@Component({
  selector: 'cd-create-cluster',
  templateUrl: './create-cluster.component.html',
  styleUrls: ['./create-cluster.component.scss']
})
export class CreateClusterComponent implements OnDestroy {
  @ViewChild('skipConfirmTpl', { static: true })
  skipConfirmTpl: TemplateRef<any>;
  currentStep: WizardStepModel;
  currentStepSub: Subscription;
  permissions: Permissions;
  projectConstants: typeof AppConstants = AppConstants;
  stepTitles = ['Add Hosts', 'Review'];
  startClusterCreation = false;
  observables: any = [];
  modalRef: NgbModalRef;

  constructor(
    private authStorageService: AuthStorageService,
    private stepsService: WizardStepsService,
    private router: Router,
    private hostService: HostService,
    private notificationService: NotificationService,
    private actionLabels: ActionLabelsI18n,
    private clusterService: ClusterService,
    private modalService: ModalService
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
    const modalVariables = {
      titleText: $localize`Warning`,
      buttonText: $localize`Continue`,
      warning: true,
      bodyTpl: this.skipConfirmTpl,
      showSubmit: true,
      onSubmit: () => {
        this.clusterService.updateStatus('POST_INSTALLED').subscribe({
          error: () => this.modalRef.close(),
          complete: () => {
            this.notificationService.show(
              NotificationType.info,
              $localize`Cluster expansion skipped by user`
            );
            this.router.navigate(['/dashboard']);
            this.modalRef.close();
          }
        });
      }
    };
    this.modalRef = this.modalService.show(ConfirmationModalComponent, modalVariables);
  }

  onSubmit() {
    forkJoin(this.observables)
      .pipe(
        finalize(() =>
          this.clusterService.updateStatus('POST_INSTALLED').subscribe(() => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Cluster expansion was successful`
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
    return !this.stepsService.isLastStep() ? this.actionLabels.NEXT : $localize`Expand Cluster`;
  }

  showCancelButtonLabel() {
    return !this.stepsService.isFirstStep() ? this.actionLabels.BACK : this.actionLabels.CANCEL;
  }

  ngOnDestroy(): void {
    this.currentStepSub.unsubscribe();
  }
}
