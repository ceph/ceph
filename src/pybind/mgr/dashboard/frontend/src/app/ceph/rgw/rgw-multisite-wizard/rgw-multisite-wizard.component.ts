import { ChangeDetectorRef, Component, NgZone, OnInit } from '@angular/core';
import { Location } from '@angular/common';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { Observable, Subscription, forkJoin } from 'rxjs';
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
import { ActivatedRoute } from '@angular/router';
import { map, switchMap } from 'rxjs/operators';
import { BaseModal, Step } from 'carbon-components-angular';
import { SummaryService } from '~/app/shared/services/summary.service';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import {
  STEP_TITLES_EXISTING_REALM,
  STEP_TITLES_MULTI_CLUSTER_CONFIGURED,
  STEP_TITLES_SINGLE_CLUSTER
} from './multisite-wizard-steps.enum';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { MultiCluster, MultiClusterConfig } from '~/app/shared/models/multi-cluster';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';

interface DaemonStats {
  rgw_metadata?: {
    [key: string]: string;
  };
}

interface EndpointInfo {
  hostname: string;
  port: number;
  frontendConfig: string;
}

enum Protocol {
  HTTP = 'http',
  HTTPS = 'https'
}

enum ConfigType {
  NewRealm = 'newRealm',
  ExistingRealm = 'existingRealm'
}

@Component({
  selector: 'cd-rgw-multisite-wizard',
  templateUrl: './rgw-multisite-wizard.component.html',
  styleUrls: ['./rgw-multisite-wizard.component.scss']
})
export class RgwMultisiteWizardComponent extends BaseModal implements OnInit {
  multisiteSetupForm: CdFormGroup;
  currentStep: WizardStepModel;
  currentStepSub: Subscription;
  permissions: Permissions;
  stepTitles: Step[] = STEP_TITLES_MULTI_CLUSTER_CONFIGURED.map((title) => ({
    label: title
  }));
  stepsToSkip: { [steps: string]: boolean } = {};
  daemons: RgwDaemon[] = [];
  selectedCluster = '';
  clusterDetailsArray: MultiCluster[] = [];
  isMultiClusterConfigured = false;
  exportTokenForm: CdFormGroup;
  realms: any;
  loading = false;
  pageURL: string;
  icons = Icons;
  rgwEndpoints: { value: any[]; options: any[]; messages: any };
  executingTask: ExecutingTask;
  setupCompleted = false;
  showConfigType = false;
  realmList: string[] = [];
  rgwModuleStatus: boolean;

  constructor(
    private wizardStepsService: WizardStepsService,
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private rgwDaemonService: RgwDaemonService,
    private multiClusterService: MultiClusterService,
    private rgwMultisiteService: RgwMultisiteService,
    private rgwRealmService: RgwRealmService,
    public notificationService: NotificationService,
    private route: ActivatedRoute,
    private summaryService: SummaryService,
    private location: Location,
    private cdr: ChangeDetectorRef,
    private mgrModuleService: MgrModuleService,
    private zone: NgZone
  ) {
    super();
    this.pageURL = 'rgw/multisite/configuration';
    this.currentStepSub = this.wizardStepsService
      .getCurrentStep()
      .subscribe((step: WizardStepModel) => {
        this.currentStep = step;
      });
    this.currentStep.stepIndex = 0;
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
    this.open = this.route.outlet === 'modal';
    this.loadRGWEndpoints();
    this.multiClusterService.getCluster().subscribe((clusters: MultiClusterConfig) => {
      const currentUrl = clusters['current_url'];
      this.clusterDetailsArray = Object.values(clusters['config'])
        .flat()
        .filter((cluster) => cluster['url'] !== currentUrl);
      this.isMultiClusterConfigured = this.clusterDetailsArray.length > 0;
      this.stepTitles = (this.isMultiClusterConfigured
        ? STEP_TITLES_MULTI_CLUSTER_CONFIGURED
        : STEP_TITLES_SINGLE_CLUSTER
      ).map((label, index) => ({
        label,
        onClick: () => (this.currentStep.stepIndex = index)
      }));
      this.wizardStepsService.setTotalSteps(this.stepTitles.length);
      this.selectedCluster = this.isMultiClusterConfigured
        ? this.clusterDetailsArray[0]['name']
        : null;
    });

    this.summaryService.subscribe((summary) => {
      this.zone.run(() => {
        this.executingTask = summary.executing_tasks.find((task) =>
          task.name.includes('progress/Multisite-Setup')
        );
        this.cdr.detectChanges();
      });
    });

    this.stepTitles.forEach((step) => {
      this.stepsToSkip[step.label] = false;
    });

    this.rgwRealmService.list().subscribe((realms: string[]) => {
      this.realmList = realms;
      this.showConfigType = this.realmList.length > 0;
      if (this.showConfigType) {
        this.multisiteSetupForm.get('selectedRealm')?.setValue(this.realmList[0]);
        this.cdr.detectChanges();
      }
    });

    this.rgwMultisiteService.getRgwModuleStatus().subscribe((status: boolean) => {
      this.rgwModuleStatus = status;
    });
  }

  private loadRGWEndpoints(): void {
    this.rgwDaemonService
      .list()
      .pipe(
        switchMap((daemons: RgwDaemon[]) => {
          this.daemons = daemons;
          return this.fetchDaemonStats(daemons);
        })
      )
      .subscribe((daemonStatsArray: EndpointInfo[]) => {
        this.populateRGWEndpoints(daemonStatsArray);
      });
  }

  private fetchDaemonStats(daemons: RgwDaemon[]): Observable<EndpointInfo[]> {
    const observables = daemons.map((daemon) =>
      this.rgwDaemonService.get(daemon.id).pipe(
        map((daemonStats: DaemonStats) => ({
          hostname: daemon.server_hostname,
          port: daemon.port,
          frontendConfig: daemonStats?.rgw_metadata?.['frontend_config#0'] || ''
        }))
      )
    );
    return forkJoin(observables);
  }

  private populateRGWEndpoints(statsArray: EndpointInfo[]): void {
    this.rgwEndpoints.value = statsArray.map((stats: EndpointInfo) => {
      const protocol = stats.frontendConfig.includes('ssl_port') ? Protocol.HTTPS : Protocol.HTTP;
      return `${protocol}://${stats.hostname}:${stats.port}`;
    });
    this.rgwEndpoints.options = this.rgwEndpoints.value.map(
      (endpoint) => new SelectOption(false, endpoint, '')
    );
    this.cdr.detectChanges();
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
      }),
      replicationZoneName: new UntypedFormControl('new_replicated_zone', {
        validators: [Validators.required]
      }),
      configType: new UntypedFormControl(ConfigType.NewRealm, {}),
      selectedRealm: new UntypedFormControl(null, {})
    });

    if (!this.isMultiClusterConfigured) {
      this.exportTokenForm = new CdFormGroup({});
    }
  }

  showSubmitButtonLabel() {
    if (this.wizardStepsService.isLastStep()) {
      if (!this.setupCompleted) {
        if (this.isMultiClusterConfigured) {
          return $localize`Configure Multi-Site`;
        } else {
          return $localize`Export Multi-Site token`;
        }
      } else {
        return $localize`Close`;
      }
    } else {
      return $localize`Next`;
    }
  }

  showCancelButtonLabel() {
    return !this.wizardStepsService.isFirstStep()
      ? this.actionLabels.BACK
      : this.actionLabels.CANCEL;
  }

  onNextStep() {
    if (!this.wizardStepsService.isLastStep()) {
      this.wizardStepsService.moveToNextStep();
    } else {
      if (this.setupCompleted) {
        this.closeModal();
      } else {
        this.onSubmit();
      }
    }
    this.wizardStepsService.getCurrentStep().subscribe((step: WizardStepModel) => {
      this.currentStep = step;
      if (this.currentStep.stepIndex === 2 && this.isMultiClusterConfigured) {
        this.stepsToSkip['Select Cluster'] = false;
      }
    });
  }

  onSubmit() {
    this.loading = true;

    const proceedWithSetup = () => {
      this.cdr.detectChanges();
      const values = this.multisiteSetupForm.getRawValue();
      const realmName = values['realmName'];
      const zonegroupName = values['zonegroupName'];
      const zonegroupEndpoints = this.rgwEndpoints.value.join(',');
      const zoneName = values['zoneName'];
      const zoneEndpoints = this.rgwEndpoints.value.join(',');
      const username = values['username'];

      if (!this.isMultiClusterConfigured || this.stepsToSkip['Select Cluster']) {
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
            this.setupCompleted = true;
            this.rgwMultisiteService.setRestartGatewayMessage(false);
            this.loading = false;
            this.realms = data;
            this.showSuccessNotification();
          });
      } else {
        const cluster = values['cluster'];
        const replicationZoneName = values['replicationZoneName'];
        let selectedRealmName = '';

        if (this.multisiteSetupForm.get('configType').value === ConfigType.ExistingRealm) {
          selectedRealmName = this.multisiteSetupForm.get('selectedRealm').value;
        }

        this.rgwMultisiteService
          .setUpMultisiteReplication(
            realmName,
            zonegroupName,
            zonegroupEndpoints,
            zoneName,
            zoneEndpoints,
            username,
            cluster,
            replicationZoneName,
            this.clusterDetailsArray,
            selectedRealmName
          )
          .subscribe(
            () => {
              this.setupCompleted = true;
              this.rgwMultisiteService.setRestartGatewayMessage(false);
              this.loading = false;
              this.showSuccessNotification();
            },
            () => {
              this.multisiteSetupForm.setErrors({ cdSubmitButton: true });
            }
          );
      }
    };

    if (!this.rgwModuleStatus) {
      this.mgrModuleService.updateModuleState(
        'rgw',
        false,
        null,
        '',
        '',
        false,
        $localize`RGW module is being enabled. Waiting for the system to reconnect...`
      );
      const subscription = this.mgrModuleService.updateCompleted$.subscribe(() => {
        subscription.unsubscribe();
        proceedWithSetup();
      });
    } else {
      proceedWithSetup();
    }
  }

  showSuccessNotification() {
    this.notificationService.show(
      NotificationType.success,
      $localize`Multi-site setup completed successfully.`
    );
  }

  onPreviousStep() {
    if (!this.wizardStepsService.isFirstStep()) {
      this.wizardStepsService.moveToPreviousStep();
    } else {
      this.location.back();
    }
  }

  onSkip() {
    const stepTitle = this.stepTitles[this.currentStep.stepIndex];
    this.stepsToSkip[stepTitle.label] = true;
    this.onNextStep();
  }

  closeModal(): void {
    this.location.back();
  }

  onConfigTypeChange() {
    const configType = this.multisiteSetupForm.get('configType')?.value;
    if (configType === ConfigType.ExistingRealm) {
      this.stepTitles = STEP_TITLES_EXISTING_REALM.map((title) => ({
        label: title
      }));
      this.stepTitles.forEach((steps, index) => {
        steps.onClick = () => (this.currentStep.stepIndex = index);
      });
    } else if (this.isMultiClusterConfigured) {
      this.stepTitles = STEP_TITLES_MULTI_CLUSTER_CONFIGURED.map((title) => ({
        label: title
      }));
    } else {
      this.stepTitles = STEP_TITLES_SINGLE_CLUSTER.map((title) => ({
        label: title
      }));
    }
    this.wizardStepsService.setTotalSteps(this.stepTitles.length);
  }
}
