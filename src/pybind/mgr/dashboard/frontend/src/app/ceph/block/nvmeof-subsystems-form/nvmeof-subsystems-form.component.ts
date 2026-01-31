import { Component, DestroyRef, OnInit, ViewChild } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { ActivatedRoute, Router } from '@angular/router';
import { Step } from 'carbon-components-angular';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { TearsheetComponent } from '~/app/shared/components/tearsheet/tearsheet.component';

export type SubsystemPayload = {
  nqn: string;
  gw_group: string;
};

@Component({
  selector: 'cd-nvmeof-subsystems-form',
  templateUrl: './nvmeof-subsystems-form.component.html',
  styleUrls: ['./nvmeof-subsystems-form.component.scss'],
  standalone: false
})
export class NvmeofSubsystemsFormComponent implements OnInit {
  subsystemForm: CdFormGroup;
  action: string;
  group: string;
  steps: Step[] = [
    {
      label: $localize`Subsystem Details`,
      complete: false,
      invalid: false
    },
    {
      label: $localize`Host access control`,
      complete: false
    },
    {
      label: $localize`Authentication`,
      complete: false
    },
    {
      label: $localize`Advanced Options`,
      complete: false,
      secondaryLabel: $localize`Advanced`
    }
  ];
  title: string = $localize`Create Subsystem`;
  description: string = $localize`Subsytems define how hosts connect to NVMe namespaces and ensure secure access to storage.`;
  isSubmitLoading: boolean = false;

  @ViewChild(TearsheetComponent) tearsheet!: TearsheetComponent;

  constructor(
    public actionLabels: ActionLabelsI18n,
    public activeModal: NgbActiveModal,
    private route: ActivatedRoute,
    private destroyRef: DestroyRef,
    private nvmeofService: NvmeofService,
    private taskWrapperService: TaskWrapperService,
    private router: Router
  ) {}

  ngOnInit() {
    this.route.queryParams.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((params) => {
      this.group = params?.['group'];
    });
  }

  onSubmit(payload: SubsystemPayload) {
    const component = this;
    const pageURL = 'block/nvmeof/subsystems';
    let taskUrl = `nvmeof/subsystem/${URLVerbs.CREATE}`;
    this.isSubmitLoading = true;
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          nqn: payload.nqn
        }),
        call: this.nvmeofService.createSubsystem({ ...payload, enable_ha: true })
      })
      .subscribe({
        error() {
          component.isSubmitLoading = false;
        },
        complete: () => {
          component.isSubmitLoading = false;
          this.router.navigate([pageURL, { outlets: { modal: null } }]);
        }
      });
  }
}
