import { Component, OnInit, ViewChild } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { NvmeofService, SubsystemInitiatorRequest } from '~/app/shared/api/nvmeof.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import {
  AuthStepType,
  HOST_TYPE,
  HostStepType,
  NvmeofSubsystemInitiator
} from '~/app/shared/models/nvmeof';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { ActivatedRoute, Router } from '@angular/router';
import { TearsheetComponent } from '~/app/shared/components/tearsheet/tearsheet.component';

type InitiatorsFormPayload = Pick<HostStepType, 'hostType' | 'addedHosts'> &
  Partial<Pick<AuthStepType, 'hostDchapKeyList'>>;

const STEP_LABELS = {
  HOSTS: $localize`Host access control`,
  AUTH: $localize`Authentication (optional)`
} as const;

@Component({
  selector: 'cd-nvmeof-initiators-form',
  templateUrl: './nvmeof-initiators-form.component.html',
  styleUrls: ['./nvmeof-initiators-form.component.scss'],
  standalone: false
})
export class NvmeofInitiatorsFormComponent implements OnInit {
  group!: string;
  subsystemNQN!: string;
  isSubmitLoading = false;
  existingHosts: string[] = [];
  showAuthStep = true;
  stepTwoValue: HostStepType = null;

  @ViewChild(TearsheetComponent) tearsheet!: TearsheetComponent;

  steps: Step[] = [];

  title = $localize`Add Initiator`;
  description = $localize`Allow specific hosts to run NVMe/TCP commands to the NVMe subsystem.`;
  pageURL = 'block/nvmeof/subsystems';

  constructor(
    private nvmeofService: NvmeofService,
    private taskWrapperService: TaskWrapperService,
    private router: Router,
    private route: ActivatedRoute
  ) {}

  ngOnInit() {
    this.route.queryParams.subscribe((params) => {
      this.group = params?.['group'];
    });
    this.route.parent.params.subscribe((params: any) => {
      if (params.subsystem_nqn) {
        this.subsystemNQN = params.subsystem_nqn;
      }
    });
    this.route.params.subscribe((params: any) => {
      if (!this.subsystemNQN && params.subsystem_nqn) {
        this.subsystemNQN = params.subsystem_nqn;
      }
      this.fetchExistingHosts();
    });
    this.rebuildSteps();
  }

  rebuildSteps() {
    const steps: Step[] = [{ label: STEP_LABELS.HOSTS, invalid: false }];

    if (this.showAuthStep) {
      steps.push({ label: STEP_LABELS.AUTH, invalid: false });
    }

    this.steps = steps;

    if (this.tearsheet?.currentStep >= steps.length) {
      this.tearsheet.currentStep = steps.length - 1;
    }
  }

  onStepChanged() {
    if (!this.tearsheet) return;

    const hostStep = this.tearsheet.getStepValueByLabel<HostStepType>(STEP_LABELS.HOSTS);

    if (hostStep) {
      this.stepTwoValue = hostStep;
    }

    const nextShowAuth = (hostStep?.hostType ?? HOST_TYPE.SPECIFIC) === HOST_TYPE.SPECIFIC;

    if (nextShowAuth !== this.showAuthStep) {
      this.showAuthStep = nextShowAuth;
      this.rebuildSteps();
    }
  }

  fetchExistingHosts() {
    if (!this.subsystemNQN || !this.group) return;
    this.nvmeofService
      .getInitiators(this.subsystemNQN, this.group)
      .subscribe((response: NvmeofSubsystemInitiator[] | { hosts: NvmeofSubsystemInitiator[] }) => {
        const initiators = Array.isArray(response) ? response : response?.hosts || [];
        this.existingHosts = initiators.map((i) => i.nqn);
      });
  }

  onSubmit(payload: InitiatorsFormPayload) {
    this.isSubmitLoading = true;
    const taskUrl = `nvmeof/initiator/add`;
    const hostKeyList = payload.hostDchapKeyList || [];
    const addedHosts = payload.addedHosts || [];
    const hosts =
      payload.hostType === HOST_TYPE.SPECIFIC
        ? hostKeyList.length
          ? hostKeyList
          : addedHosts.map((host_nqn: string) => ({ host_nqn, dhchap_key: '' }))
        : [];

    const request: SubsystemInitiatorRequest = {
      allow_all: payload.hostType === HOST_TYPE.ALL,
      hosts,
      gw_group: this.group
    };
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          nqn: this.subsystemNQN
        }),
        call: this.nvmeofService.addSubsystemInitiators(this.subsystemNQN, request)
      })
      .subscribe({
        error: () => {
          this.isSubmitLoading = false;
        },
        complete: () => {
          this.isSubmitLoading = false;
          this.router.navigate([{ outlets: { modal: null } }], {
            relativeTo: this.route.parent,
            queryParamsHandling: 'preserve'
          });
        }
      });
  }
}
