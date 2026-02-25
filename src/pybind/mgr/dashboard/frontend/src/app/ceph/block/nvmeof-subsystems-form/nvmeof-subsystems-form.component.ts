import { Component, DestroyRef, OnInit, SecurityContext, ViewChild } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { ActivatedRoute, Router } from '@angular/router';
import { Step } from 'carbon-components-angular';
import { InitiatorRequest, NvmeofService } from '~/app/shared/api/nvmeof.service';
import { TearsheetComponent } from '~/app/shared/components/tearsheet/tearsheet.component';
import { HOST_TYPE, ListenerItem } from '~/app/shared/models/nvmeof';
import { from, Observable, of } from 'rxjs';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { catchError, concatMap, map, tap } from 'rxjs/operators';
import { DomSanitizer } from '@angular/platform-browser';

export type SubsystemPayload = {
  nqn: string;
  gw_group: string;
  subsystemDchapKey: string;
  addedHosts: string[];
  hostType: string;
  listeners: ListenerItem[];
};

type StepResult = { step: string; success: boolean; error?: string };

@Component({
  selector: 'cd-nvmeof-subsystems-form',
  templateUrl: './nvmeof-subsystems-form.component.html',
  styleUrls: ['./nvmeof-subsystems-form.component.scss'],
  standalone: false
})
export class NvmeofSubsystemsFormComponent implements OnInit {
  action: string;
  group: string;
  steps: Step[] = [
    {
      label: $localize`Subsystem details`,
      complete: false,
      invalid: false
    },
    {
      label: $localize`Host access control`,
      invalid: false
    },
    {
      label: $localize`Authentication`,
      complete: false
    }
  ];
  title: string = $localize`Create Subsystem`;
  description: string = $localize`Subsytems define how hosts connect to NVMe namespaces and ensure secure access to storage.`;
  isSubmitLoading: boolean = false;
  private lastCreatedNqn: string;

  @ViewChild(TearsheetComponent) tearsheet!: TearsheetComponent;

  constructor(
    public actionLabels: ActionLabelsI18n,
    public activeModal: NgbActiveModal,
    private route: ActivatedRoute,
    private destroyRef: DestroyRef,
    private nvmeofService: NvmeofService,
    private notificationService: NotificationService,
    private router: Router,
    private sanitizer: DomSanitizer
  ) {}

  ngOnInit() {
    this.route.queryParams.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((params) => {
      this.group = params?.['group'];
    });
  }
  onSubmit(payload: SubsystemPayload) {
    this.isSubmitLoading = true;
    this.lastCreatedNqn = payload.nqn;
    const stepResults: StepResult[] = [];
    const initiatorRequest: InitiatorRequest = {
      host_nqn: payload.hostType === HOST_TYPE.ALL ? '*' : payload.addedHosts.join(','),
      gw_group: this.group
    };
    this.nvmeofService
      .createSubsystem({
        nqn: payload.nqn,
        gw_group: this.group,
        enable_ha: true,
        dhchap_key: payload.subsystemDchapKey
      })
      .subscribe({
        next: () => {
          stepResults.push({ step: this.steps[0].label, success: true });
          const sequentialSteps: { step: string; call: () => Observable<any> }[] = [];

          if (payload.listeners && payload.listeners.length > 0) {
            sequentialSteps.push({
              step: $localize`Listeners`,
              call: () =>
                this.nvmeofService.createListeners(
                  `${payload.nqn}.${this.group}`,
                  this.group,
                  payload.listeners
                )
            });
          }

          sequentialSteps.push({
            step: this.steps[1].label,
            call: () =>
              this.nvmeofService.addInitiators(`${payload.nqn}.${this.group}`, initiatorRequest)
          });

          this.runSequentialSteps(sequentialSteps, stepResults).subscribe({
            complete: () => this.showFinalNotification(stepResults)
          });
        },
        error: (err) => {
          err.preventDefault();
          const errorMsg = err?.error?.detail || $localize`Subsystem creation failed`;
          this.notificationService.show(
            NotificationType.error,
            $localize`Subsystem creation failed`,
            errorMsg
          );
          this.isSubmitLoading = false;
          this.router.navigate(['block/nvmeof/gateways'], {
            queryParams: { group: this.group, tab: 'subsystem' }
          });
        }
      });
  }

  private runSequentialSteps(
    steps: { step: string; call: () => Observable<any> }[],
    stepResults: StepResult[]
  ): Observable<void> {
    return from(steps).pipe(
      concatMap((step) =>
        step.call().pipe(
          tap(() => stepResults.push({ step: step.step, success: true })),
          catchError((err) => {
            err.preventDefault();
            const errorMsg = err?.error?.detail || '';
            stepResults.push({ step: step.step, success: false, error: errorMsg });
            return of(null);
          })
        )
      ),
      map(() => void 0)
    );
  }

  private showFinalNotification(stepResults: StepResult[]) {
    this.isSubmitLoading = false;

    const messageLines = stepResults.map((stepResult) =>
      stepResult.success
        ? $localize`<div>${stepResult.step} step created successfully</div><br/>`
        : $localize`<div>${stepResult.step} step failed: <code>${stepResult.error}</code></div><br/>`
    );

    const rawHtml = messageLines.join('<br/>');
    const sanitizedHtml = this.sanitizer.sanitize(SecurityContext.HTML, rawHtml) ?? '';

    const hasFailure = stepResults.some((r) => !r.success);
    const type = hasFailure ? NotificationType.error : NotificationType.success;
    const title = hasFailure
      ? $localize`Subsystem created (with errors)`
      : $localize`Subsystem created`;

    this.notificationService.show(type, title, sanitizedHtml);
    this.router.navigate(['block/nvmeof/gateways'], {
      queryParams: {
        group: this.group,
        tab: 'subsystem',
        nqn: stepResults[0]?.success ? this.lastCreatedNqn : null
      }
    });
  }
}
