import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { Subscription } from 'rxjs';
import { ActionLabelsI18n, AppConstants } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { DeploymentOptions } from '~/app/shared/models/osd-deployment-options';
import { WizardStepModel } from '~/app/shared/models/wizard-steps';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { DriveGroup } from '../../cluster/osd/osd-form/drive-group.model';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwDaemon } from '../models/rgw-daemon';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwRealm } from '../models/rgw-multisite';
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import _ from 'lodash';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';

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
  projectConstants: typeof AppConstants = AppConstants;
  stepTitles = ['Create Realm & Zonegroup', 'Create Zone', 'Select Cluster'];
  startClusterCreation = false;
  observables: any = [];
  driveGroup = new DriveGroup();
  driveGroups: Object[] = [];
  deploymentOption: DeploymentOptions;
  selectedOption = {};
  simpleDeployment = true;
  stepsToSkip: { [steps: string]: boolean } = {};
  daemons: RgwDaemon[] = [];
  rgwEndpointsArray: string[] = [];
  selectedEndpoint = '';
  selectedCluster = '';
  clusterDetailsArray: any;
  isMultiCluster = false;
  exportTokenForm: CdFormGroup;
  realms: any;
  realmList: RgwRealm[];
  multisiteInfo: any;
  tokenValid = false;
  loading = false;
  icons = Icons;
  multisiteCreatedWithToken = false;
  labelsOption: Array<SelectOption> = [];
  rgwEndpoints: { value: any[]; options: any[]; messages: any };

  constructor(
    private wizardStepsService: WizardStepsService,
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private rgwDaemonService: RgwDaemonService,
    private multiClusterService: MultiClusterService,
    private rgwMultisiteService: RgwMultisiteService
  ) {
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
        filter: $localize`Add endpoints`
      })
    };
  }

  ngOnInit(): void {
    this.rgwDaemonService.list().subscribe((daemons) => {
      this.daemons = daemons;
      this.daemons.forEach((daemon) => {
        this.rgwEndpoints.value.push('http://' + daemon.server_hostname + ':' + daemon.port);

        const options: SelectOption[] = [];
        _.forEach(this.rgwEndpoints.value, (endpoint: string) => {
          const option = new SelectOption(false, endpoint, '');
          options.push(option);
        });
        this.rgwEndpoints.options = [...options];
      });
    });

    this.multiClusterService.getCluster().subscribe((clusters) => {
      this.clusterDetailsArray = Object.values(clusters['config'])
        .flat()
        .filter((cluster) => cluster['cluster_alias'] !== 'local-cluster');
      this.isMultiCluster = this.clusterDetailsArray.length > 0;
      if (!this.isMultiCluster) {
        this.stepTitles = ['Create Realm & Zonegorup', 'Create Zone', 'Export Multi-Site token'];
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

    if (!this.isMultiCluster) {
      this.exportTokenForm = new CdFormGroup({});
    }
  }

  showSubmitButtonLabel() {
    if (this.isMultiCluster) {
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
      if (this.currentStep.stepIndex === 2 && !this.isMultiCluster) {
        this.loading = true;
        this.onSubmit();
      } else {
        this.wizardStepsService.moveToNextStep();
      }
    } else {
      this.loading = true;
      this.onSubmit();
    }
  }

  onSubmit() {
    const values = this.multisiteSetupForm.value;
    const realmName = values['realmName'];
    const zonegroupName = values['zonegroupName'];
    const zonegroup_endpoints = this.rgwEndpoints.value.join(',');
    const zoneName = values['zoneName'];
    const zone_endpoints = this.rgwEndpoints.value.join(',');
    const username = values['username'];
    if (!this.isMultiCluster) {
      if (this.wizardStepsService.isLastStep()) {
        this.activeModal.close();
      } else {
        this.rgwMultisiteService
          .setUpMultisiteFromWizard(
            realmName,
            zonegroupName,
            zonegroup_endpoints,
            zoneName,
            zone_endpoints,
            username
          )
          .subscribe((data: object[]) => {
            this.loading = false;
            this.realms = data;
            this.wizardStepsService.moveToNextStep();
          });
      }
    } else {
      const cluster = values['cluster'];
      this.rgwMultisiteService
        .setUpMultisiteFromWizard(
          realmName,
          zonegroupName,
          zonegroup_endpoints,
          zoneName,
          zone_endpoints,
          username,
          cluster
        )
        .subscribe(
          () => {
            this.activeModal.close();
          },
          () => {
            this.multisiteSetupForm.setErrors({ cdSubmitButton: true });
          }
        );
    }
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
