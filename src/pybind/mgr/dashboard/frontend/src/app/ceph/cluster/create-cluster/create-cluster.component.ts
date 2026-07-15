import {
  Component,
  OnDestroy,
  OnInit,
  TemplateRef,
  ViewChild,
  ViewEncapsulation
} from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { forkJoin } from 'rxjs';
import { finalize } from 'rxjs/operators';
import { Step } from 'carbon-components-angular';

import { ClusterService } from '~/app/shared/api/cluster.service';
import { HostService } from '~/app/shared/api/host.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { TearsheetComponent } from '~/app/shared/components/tearsheet/tearsheet.component';
import { AppConstants, URLVerbs } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { DeploymentOptions } from '~/app/shared/models/osd-deployment-options';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { DriveGroup } from '../osd/osd-form/drive-group.model';
import { Icons } from '~/app/shared/enum/icons.enum';

const STEP_LABELS = {
  ADD_HOSTS: $localize`Add Hosts`,
  CREATE_OSDS: $localize`Create OSDs`,
  CREATE_SERVICES: $localize`Create Services`,
  REVIEW: $localize`Review`
} as const;

@Component({
  selector: 'cd-create-cluster',
  templateUrl: './create-cluster.component.html',
  styleUrls: ['./create-cluster.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class CreateClusterComponent implements OnInit, OnDestroy {
  @ViewChild('skipConfirmTpl', { static: true })
  skipConfirmTpl: TemplateRef<any>;
  @ViewChild(TearsheetComponent) tearsheet!: TearsheetComponent;

  permissions: Permissions;
  projectConstants: typeof AppConstants = AppConstants;
  steps: Step[] = [
    { label: STEP_LABELS.ADD_HOSTS, invalid: false },
    { label: STEP_LABELS.CREATE_OSDS, invalid: false },
    { label: STEP_LABELS.CREATE_SERVICES, invalid: false },
    { label: STEP_LABELS.REVIEW, invalid: false }
  ];
  title = $localize`Add Storage`;
  description = $localize`Configure hosts, OSDs, and data services for your cluster.`;
  submitButtonLabel = $localize`Add Storage`;
  isSubmitLoading = false;

  startClusterCreation = true;
  observables: any = [];
  modalRef: NgbModalRef;
  driveGroup = new DriveGroup();
  driveGroups: Object[] = [];
  deploymentOption: DeploymentOptions;
  selectedOption = {};
  simpleDeployment = true;
  stepsToSkip: { [steps: string]: boolean } = {};
  icons = Icons;

  constructor(
    private authStorageService: AuthStorageService,
    private router: Router,
    private hostService: HostService,
    private notificationService: NotificationService,
    private clusterService: ClusterService,
    private modalService: ModalCdsService,
    private taskWrapper: TaskWrapperService,
    private osdService: OsdService,
    private route: ActivatedRoute
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.route.queryParams.subscribe((params) => {
      const showWelcomeScreen = params['welcome'];
      if (showWelcomeScreen) {
        this.startClusterCreation = showWelcomeScreen;
      }
    });

    this.osdService.getDeploymentOptions().subscribe((options) => {
      this.deploymentOption = options;
      this.selectedOption = { option: options.recommended_option, encrypted: false };
    });

    this.steps.forEach((step) => {
      this.stepsToSkip[step.label] = false;
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
          error: () => this.modalService.dismissAll(),
          complete: () => {
            this.notificationService.show(
              NotificationType.info,
              $localize`Storage setup skipped by user`
            );
            this.router.navigate(['/overview']);
            this.modalService.dismissAll();
          }
        });
      }
    };
    this.modalService.show(ConfirmationModalComponent, modalVariables);
  }

  onSkipOsdStep() {
    this.stepsToSkip[STEP_LABELS.CREATE_OSDS] = true;
    this.tearsheet.onNext();
  }

  onSubmit() {
    const osdStepData = this.tearsheet?.getStepValueByLabel<{
      skipped: boolean;
      driveGroup: DriveGroup;
      selectedOption: object;
      simpleDeployment: boolean;
    }>(STEP_LABELS.CREATE_OSDS);

    if (osdStepData?.skipped) {
      this.stepsToSkip[STEP_LABELS.CREATE_OSDS] = true;
    } else if (osdStepData) {
      if (osdStepData.driveGroup) {
        this.driveGroup = osdStepData.driveGroup;
      }
      if (osdStepData.selectedOption) {
        this.selectedOption = osdStepData.selectedOption;
      }
      if (osdStepData.simpleDeployment !== undefined) {
        this.simpleDeployment = osdStepData.simpleDeployment;
      }
    }

    if (!this.stepsToSkip[STEP_LABELS.ADD_HOSTS]) {
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
                this.router.navigate(['/overview']);
              })
            )
          )
          .subscribe({
            error: (error) => error.preventDefault()
          });
      });
    }

    if (!this.stepsToSkip[STEP_LABELS.CREATE_OSDS]) {
      if (this.driveGroup) {
        const user = this.authStorageService.getUsername();
        this.driveGroup.setName(`dashboard-${user}-${_.now()}`);
        this.driveGroups.push(this.driveGroup.spec);
      }

      if (this.simpleDeployment) {
        const title = this.deploymentOption?.options[this.selectedOption['option']]?.title;
        const trackingId = $localize`${title} deployment`;
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('osd/' + URLVerbs.CREATE, {
              tracking_id: trackingId
            }),
            call: this.osdService.create([this.selectedOption], trackingId, 'predefined')
          })
          .subscribe({
            error: (error) => error.preventDefault()
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
                this.osdService.osdDevices = [];
              }
            });
        }
      }
    }
  }

  ngOnDestroy(): void {
    this.osdService.selectedFormValues = null;
  }
}
