import { Component, OnInit } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { InitiatorRequest, NvmeofService } from '~/app/shared/api/nvmeof.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { HOST_TYPE, NvmeofSubsystemInitiator } from '~/app/shared/models/nvmeof';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { ActivatedRoute, Router } from '@angular/router';
import { SubsystemPayload } from '../nvmeof-subsystems-form/nvmeof-subsystems-form.component';

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

  steps: Step[] = [
    {
      label: $localize`Host access control`,
      invalid: false
    }
  ];

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

  onSubmit(payload: SubsystemPayload) {
    this.isSubmitLoading = true;
    const taskUrl = `nvmeof/initiator/add`;

    const request: InitiatorRequest = {
      host_nqn: payload.hostType === HOST_TYPE.ALL ? '*' : payload.addedHosts.join(','),
      gw_group: this.group
    };
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          nqn: this.subsystemNQN
        }),
        call: this.nvmeofService.addInitiators(this.subsystemNQN, request)
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
