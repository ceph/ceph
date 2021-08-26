import { Component, EventEmitter, OnDestroy, Output, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { forkJoin, Subscription } from 'rxjs';
import { finalize } from 'rxjs/operators';

import { ClusterService } from '~/app/shared/api/cluster.service';
import { HostService } from '~/app/shared/api/host.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { ActionLabelsI18n, AppConstants, URLVerbs } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permissions } from '~/app/shared/models/permissions';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { DriveGroup } from '../osd/osd-form/drive-group.model';

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
  stepTitles = ['Add Hosts', 'Create OSDs', 'Review'];
  startClusterCreation = false;
  observables: any = [];
  modalRef: NgbModalRef;
  driveGroup = new DriveGroup();
  driveGroups: Object[] = [];

  @Output()
  submitAction = new EventEmitter();

  constructor(
    private authStorageService: AuthStorageService,
    private stepsService: WizardStepsService,
    private router: Router,
    private hostService: HostService,
    private notificationService: NotificationService,
    private actionLabels: ActionLabelsI18n,
    private clusterService: ClusterService,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService,
    private osdService: OsdService,
    private wizardStepService: WizardStepsService
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

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('osd/' + URLVerbs.CREATE, {
          tracking_id: _.join(_.map(this.driveGroups, 'service_id'), ', ')
        }),
        call: this.osdService.create(this.driveGroups)
      })
      .subscribe({
        error: (error) => error.preventDefault(),
        complete: () => {
          this.submitAction.emit();
        }
      });
  }

  onNextStep() {
    if (!this.stepsService.isLastStep()) {
      this.hostService.list().subscribe((hosts) => {
        hosts.forEach((host) => {
          const index = host['labels'].indexOf('_no_schedule', 0);
          if (index > -1) {
            host['labels'].splice(index, 1);
            this.observables.push(this.hostService.update(host['hostname'], true, host['labels']));
          }
        });
      });
      this.driveGroup = this.wizardStepService.sharedData;
      this.stepsService.getCurrentStep().subscribe((step: WizardStepModel) => {
        this.currentStep = step;
      });
      if (this.currentStep.stepIndex === 2 && this.driveGroup) {
        const user = this.authStorageService.getUsername();
        this.driveGroup.setName(`dashboard-${user}-${_.now()}`);
        this.driveGroups.push(this.driveGroup.spec);
      }
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
