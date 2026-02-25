import { Component, OnInit } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ListenerItem } from '~/app/shared/models/nvmeof';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { ActivatedRoute, Router } from '@angular/router';

@Component({
  selector: 'cd-nvmeof-listeners-form',
  templateUrl: './nvmeof-listeners-form.component.html',
  styleUrls: ['./nvmeof-listeners-form.component.scss'],
  standalone: false
})
export class NvmeofListenersFormComponent implements OnInit {
  group!: string;
  subsystemNQN!: string;
  isSubmitLoading = false;

  steps: Step[] = [
    {
      label: $localize`Listeners`,
      invalid: false
    }
  ];

  title = $localize`Add Listener`;
  description = $localize`Listeners determine where and how hosts can connect to the subsystem over the network.`;

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
    // subsystem_nqn can be in route.params (create/:subsystem_nqn/listener)
    // or route.parent.params (subsystems/:subsystem_nqn > add/listener)
    const params = this.route.snapshot.params;
    const parentParams = this.route.parent?.snapshot.params;
    this.subsystemNQN = params?.['subsystem_nqn'] || parentParams?.['subsystem_nqn'];
  }

  onSubmit(payload: { listeners: ListenerItem[] }) {
    if (!payload.listeners || payload.listeners.length === 0) {
      return;
    }
    this.isSubmitLoading = true;
    const taskUrl = `nvmeof/listener/add`;

    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          nqn: this.subsystemNQN,
          count: payload.listeners.length
        }),
        call: this.nvmeofService.createListeners(this.subsystemNQN, this.group, payload.listeners)
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
