import {
  Component,
  EventEmitter,
  OnDestroy,
  OnInit,
  Output,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

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
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { DeploymentOptions } from '~/app/shared/models/osd-deployment-options';
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
export class CreateClusterComponent implements OnInit, OnDestroy {
  @ViewChild('skipConfirmTpl', { static: true })
  skipConfirmTpl: TemplateRef<any>;
  currentStep: WizardStepModel;
  currentStepSub: Subscription;
  permissions: Permissions;
  projectConstants: typeof AppConstants = AppConstants;
  stepTitles = ['Add Hosts', 'Create OSDs', 'Create Services', 'Review'];
  startClusterCreation = false;
  observables: any = [];
  modalRef: NgbModalRef;
  driveGroup = new DriveGroup();
  driveGroups: Object[] = [];
  deploymentOption: DeploymentOptions;
  selectedOption = {};
  simpleDeployment = true;
  stepsToSkip: { [steps: string]: boolean } = {};

  @Output()
  submitAction = new EventEmitter();

  constructor(
    private authStorageService: AuthStorageService,
    private wizardStepsService: WizardStepsService,
    private router: Router,
    private hostService: HostService,
    private notificationService: NotificationService,
    private actionLabels: ActionLabelsI18n,
    private clusterService: ClusterService,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService,
    private osdService: OsdService,
    private route: ActivatedRoute
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.currentStepSub = this.wizardStepsService
      .getCurrentStep()
      .subscribe((step: WizardStepModel) => {
        this.currentStep = step;
      });
    this.currentStep.stepIndex = 1;
  }

  ngOnInit(): void {
    this.route.queryParams.subscribe((params) => {
      // reading 'welcome' value true/false to toggle expand-cluster wizand view and welcome view
      const showWelcomeScreen = params['welcome'];
      if (showWelcomeScreen) {
        this.startClusterCreation = showWelcomeScreen;
      }
    });

    this.osdService.getDeploymentOptions().subscribe((options) => {
      this.deploymentOption = options;
      this.selectedOption = { option: options.recommended_option, encrypted: false };
    });

    this.stepTitles.forEach((stepTitle) => {
      this.stepsToSkip[stepTitle] = false;
    });
  }

  createCluster() {
    this.startClusterCreation = false;
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
    if (!this.stepsToSkip['Add Hosts']) {
      const hostContext = new CdTableFetchDataContext(() => undefined);
      this.hostService.list(hostContext.toParams(), 'false').subscribe((hosts) => {
        hosts.forEach((host) => {
          const index = host['labels'].indexOf('_no_schedule', 0);
          if (index > -1) {
            host['labels'].splice(index, 1);
            this.observables.push(this.hostService.update(host['hostname'], true, host['labels']));
          }
        });
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
      });
    }

    if (!this.stepsToSkip['Create OSDs']) {
      if (this.driveGroup) {
        const user = this.authStorageService.getUsername();
        this.driveGroup.setName(`dashboard-${user}-${_.now()}`);
        this.driveGroups.push(this.driveGroup.spec);
      }

      if (this.simpleDeployment) {
        const title = this.deploymentOption?.options[this.selectedOption['option']].title;
        const trackingId = $localize`${title} deployment`;
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('osd/' + URLVerbs.CREATE, {
              tracking_id: trackingId
            }),
            call: this.osdService.create([this.selectedOption], trackingId, 'predefined')
          })
          .subscribe({
            error: (error) => error.preventDefault(),
            complete: () => {
              this.submitAction.emit();
            }
          });
      } else {
        if (this.osdService.osdDevices['totalDevices'] > 0) {
          this.driveGroup.setFeature('encrypted', this.selectedOption['encrypted']);
          const trackingId = _.join(_.map(this.driveGroups, 'service_id'), ', ');
          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('osd/' + URLVerbs.CREATE, {
                tracking_id: trackingId
              }),
              call: this.osdService.create(this.driveGroups, trackingId)
            })
            .subscribe({
              error: (error) => error.preventDefault(),
              complete: () => {
                this.submitAction.emit();
                this.osdService.osdDevices = [];
              }
            });
        }
      }
    }
  }

  setDriveGroup(driveGroup: DriveGroup) {
    this.driveGroup = driveGroup;
  }

  setDeploymentOptions(option: object) {
    this.selectedOption = option;
  }

  setDeploymentMode(mode: boolean) {
    this.simpleDeployment = mode;
  }

  onNextStep() {
    if (!this.wizardStepsService.isLastStep()) {
      this.wizardStepsService.getCurrentStep().subscribe((step: WizardStepModel) => {
        this.currentStep = step;
      });
      this.wizardStepsService.moveToNextStep();
    } else {
      this.onSubmit();
    }
  }

  onPreviousStep() {
    if (!this.wizardStepsService.isFirstStep()) {
      this.wizardStepsService.moveToPreviousStep();
    } else {
      this.router.navigate(['/dashboard']);
    }
  }

  onSkip() {
    const stepTitle = this.stepTitles[this.currentStep.stepIndex - 1];
    this.stepsToSkip[stepTitle] = true;
    this.onNextStep();
  }

  showSubmitButtonLabel() {
    return !this.wizardStepsService.isLastStep()
      ? this.actionLabels.NEXT
      : $localize`Expand Cluster`;
  }

  showCancelButtonLabel() {
    return !this.wizardStepsService.isFirstStep()
      ? this.actionLabels.BACK
      : this.actionLabels.CANCEL;
  }

  ngOnDestroy(): void {
    this.currentStepSub.unsubscribe();
    this.osdService.selectedFormValues = null;
  }
}
