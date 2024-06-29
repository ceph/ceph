import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { Subscription, forkJoin } from 'rxjs';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwDaemon } from '../models/rgw-daemon';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import _ from 'lodash';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { Router } from '@angular/router';
import { map, switchMap } from 'rxjs/operators';

@Component({
  selector: 'cd-rgw-multisite-wizard',
  templateUrl: './rgw-multisite-wizard.component.html',
  styleUrls: ['./rgw-multisite-wizard.component.scss']
})
export class RgwMultisiteWizardComponent implements OnInit {
  multisiteSetupForm: CdFormGroup;
  currentStep: WizardStepModel;
  currentStepSub: Subscription;
  permissions: Permissions;
  stepTitles = ['Create Realm & Zonegroup', 'Create Zone', 'Select Cluster'];
  stepsToSkip: { [steps: string]: boolean } = {};
  daemons: RgwDaemon[] = [];
  selectedCluster = '';
  clusterDetailsArray: any;
  isMultiClusterConfigured = false;
  exportTokenForm: CdFormGroup;
  realms: any;
  loading = false;
  pageURL: string;
  icons = Icons;
  rgwEndpoints: { value: any[]; options: any[]; messages: any };

  constructor(
    private wizardStepsService: WizardStepsService,
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private rgwDaemonService: RgwDaemonService,
    private multiClusterService: MultiClusterService,
    private rgwMultisiteService: RgwMultisiteService,
    public notificationService: NotificationService,
    private router: Router
  ) {
    this.pageURL = 'rgw/multisite';
    this.currentStepSub = this.wizardStepsService
      .getCurrentStep()
      .subscribe((step: WizardStepModel) => {
        this.currentStep = step;
      });
    this.currentStep.stepIndex = 1;
    this.createForm();
    this.rgwEndpoints = {
      value: [],
      options: [],
      messages: new SelectMessages({
        empty: $localize`There are no endpoints.`,
        filter: $localize`Select endpoints`
      })
    };
  }

  ngOnInit(): void {
    this.rgwDaemonService
      .list()
      .pipe(
        switchMap((daemons) => {
          this.daemons = daemons;
          const daemonStatsObservables = daemons.map((daemon) =>
            this.rgwDaemonService.get(daemon.id).pipe(
              map((daemonStats) => ({
                hostname: daemon.server_hostname,
                port: daemon.port,
                frontendConfig: daemonStats['rgw_metadata']['frontend_config#0']
              }))
            )
          );
          return forkJoin(daemonStatsObservables);
        })
      )
      .subscribe((daemonStatsArray) => {
        this.rgwEndpoints.value = daemonStatsArray.map((daemonStats) => {
          const protocol = daemonStats.frontendConfig.includes('ssl_port') ? 'https' : 'http';
          return `${protocol}://${daemonStats.hostname}:${daemonStats.port}`;
        });
        const options: SelectOption[] = this.rgwEndpoints.value.map(
          (endpoint: string) => new SelectOption(false, endpoint, '')
        );
        this.rgwEndpoints.options = [...options];
      });

    this.multiClusterService.getCluster().subscribe((clusters) => {
      this.clusterDetailsArray = Object.values(clusters['config'])
        .flat()
        .filter((cluster) => cluster['cluster_alias'] !== 'local-cluster');
      this.isMultiClusterConfigured = this.clusterDetailsArray.length > 0;
      if (!this.isMultiClusterConfigured) {
        this.stepTitles = ['Create Realm & Zonegroup', 'Create Zone', 'Export Multi-site token'];
      } else {
        this.selectedCluster = this.clusterDetailsArray[0]['name'];
      }
    });
  }

  createForm() {
    this.multisiteSetupForm = new CdFormGroup({
      realmName: new UntypedFormControl('default_realm', {
        validators: [Validators.required]
      }),
      zonegroupName: new UntypedFormControl('default_zonegroup', {
        validators: [Validators.required]
      }),
      zonegroup_endpoints: new UntypedFormControl(null, [Validators.required]),
      zoneName: new UntypedFormControl('default_zone', {
        validators: [Validators.required]
      }),
      zone_endpoints: new UntypedFormControl(null, {
        validators: [Validators.required]
      }),
      username: new UntypedFormControl('default_system_user', {
        validators: [Validators.required]
      }),
      cluster: new UntypedFormControl(null, {
        validators: [Validators.required]
      })
    });

    if (!this.isMultiClusterConfigured) {
      this.exportTokenForm = new CdFormGroup({});
    }
  }

  showSubmitButtonLabel() {
    if (this.isMultiClusterConfigured) {
      return !this.wizardStepsService.isLastStep()
        ? this.actionLabels.NEXT
        : $localize`Configure Multi-site`;
    } else {
      return !this.wizardStepsService.isLastStep() ? this.actionLabels.NEXT : $localize`Close`;
    }
  }

  showCancelButtonLabel() {
    return !this.wizardStepsService.isFirstStep()
      ? this.actionLabels.BACK
      : this.actionLabels.CANCEL;
  }

  onNextStep() {
    if (!this.wizardStepsService.isLastStep()) {
      this.wizardStepsService.getCurrentStep().subscribe((step: WizardStepModel) => {
        this.currentStep = step;
      });
      if (this.currentStep.stepIndex === 2 && !this.isMultiClusterConfigured) {
        this.onSubmit();
      } else {
        this.wizardStepsService.moveToNextStep();
      }
    } else {
      this.onSubmit();
    }
  }

  onSubmit() {
    this.loading = true;
    const values = this.multisiteSetupForm.value;
    const realmName = values['realmName'];
    const zonegroupName = values['zonegroupName'];
    const zonegroupEndpoints = this.rgwEndpoints.value.join(',');
    const zoneName = values['zoneName'];
    const zoneEndpoints = this.rgwEndpoints.value.join(',');
    const username = values['username'];
    if (!this.isMultiClusterConfigured) {
      if (this.wizardStepsService.isLastStep()) {
        this.activeModal.close();
        this.refreshMultisitePage();
      } else {
        this.rgwMultisiteService
          .setUpMultisiteReplication(
            realmName,
            zonegroupName,
            zonegroupEndpoints,
            zoneName,
            zoneEndpoints,
            username
          )
          .subscribe((data: object[]) => {
            this.loading = false;
            this.realms = data;
            this.wizardStepsService.moveToNextStep();
            this.showSuccessNotification();
          });
      }
    } else {
      const cluster = values['cluster'];
      this.rgwMultisiteService
        .setUpMultisiteReplication(
          realmName,
          zonegroupName,
          zonegroupEndpoints,
          zoneName,
          zoneEndpoints,
          username,
          cluster
        )
        .subscribe(
          () => {
            this.showSuccessNotification();
            this.activeModal.close();
            this.refreshMultisitePage();
          },
          () => {
            this.multisiteSetupForm.setErrors({ cdSubmitButton: true });
          }
        );
    }
  }

  showSuccessNotification() {
    this.notificationService.show(
      NotificationType.success,
      $localize`Multi-site setup completed successfully.`
    );
  }

  refreshMultisitePage() {
    const currentRoute = this.router.url.split('?')[0];
    const navigateTo = currentRoute.includes('multisite') ? '/pool' : '/';
    this.router.navigateByUrl(navigateTo, { skipLocationChange: true }).then(() => {
      this.router.navigate([currentRoute]);
    });
  }

  onPreviousStep() {
    if (!this.wizardStepsService.isFirstStep()) {
      this.wizardStepsService.moveToPreviousStep();
    } else {
      this.activeModal.close();
    }
  }

  onSkip() {
    const stepTitle = this.stepTitles[this.currentStep.stepIndex - 1];
    this.stepsToSkip[stepTitle] = true;
    this.onNextStep();
  }
}
