import { Component, DestroyRef, OnInit, SecurityContext, ViewChild } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { ActivatedRoute, Router } from '@angular/router';
import { Step } from 'carbon-components-angular';
import { NvmeofService, SubsystemInitiatorRequest } from '~/app/shared/api/nvmeof.service';
import { TearsheetComponent } from '~/app/shared/components/tearsheet/tearsheet.component';
import { HOST_TYPE, ListenerItem, AUTHENTICATION } from '~/app/shared/models/nvmeof';
import { AUTHENTICATION, HOST_TYPE, StepTwoType } from '~/app/shared/models/nvmeof';
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
  authType: AUTHENTICATION.Bidirectional | AUTHENTICATION.Unidirectional;
  hostDchapKeyList: Array<{ dhchap_key: string; host_nqn: string }>;
};

type StepResult = { step: string; success: boolean; error?: string };

@Component({
  selector: 'cd-nvmeof-subsystems-form',
  templateUrl: './nvmeof-subsystems-form.component.html',
  styleUrls: ['./nvmeof-subsystems-form.component.scss']
})
export class NvmeofSubsystemsFormComponent implements OnInit {
  action: string;
  group: string;
  steps: Step[] = [];
  title: string = $localize`Create Subsystem`;
  description: string = $localize`Subsytems define how hosts connect to NVMe namespaces and ensure secure access to storage.`;
  isSubmitLoading: boolean = false;
  private lastCreatedNqn: string;
  stepTwoValue: StepTwoType = null;
  showAuthStep = true;

  @ViewChild(TearsheetComponent) tearsheet!: TearsheetComponent;

  // Review step data
  reviewNqn: string = '';
  reviewListeners: any[] = [];
  reviewHostType: string = HOST_TYPE.SPECIFIC;
  reviewAddedHosts: string[] = [];
  reviewAuthType: string = AUTHENTICATION.Unidirectional;
  reviewSubsystemDchapKey: string = '';
  reviewHostDchapKeyCount: number = 0;

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
    this.rebuildSteps();
  }

  populateReviewData() {
    if (!this.tearsheet?.stepContents) return;
    const steps = this.tearsheet.stepContents.toArray();

    // Step 1: Subsystem details
    const step1Form = steps[0]?.stepComponent?.formGroup;
    if (step1Form) {
      this.reviewNqn = step1Form.get('nqn')?.value || '';
      this.reviewListeners = step1Form.get('listeners')?.value || [];
    }

    // Step 2: Host access control
    const step2Form = steps[1]?.stepComponent?.formGroup;
    if (step2Form) {
      this.reviewHostType = step2Form.get('hostType')?.value || HOST_TYPE.SPECIFIC;
      this.reviewAddedHosts = step2Form.get('addedHosts')?.value || [];
    }

    // Step 3: Authentication
    const step3Form = steps[2]?.stepComponent?.formGroup;
    if (step3Form) {
      this.reviewAuthType = step3Form.get('authType')?.value || AUTHENTICATION.Unidirectional;
      this.reviewSubsystemDchapKey = step3Form.get('subsystemDchapKey')?.value || '';
      const hostKeys = step3Form.get('hostDchapKeyList')?.value || [];
      this.reviewHostDchapKeyCount = hostKeys.filter((k: any) => k?.key).length;
  
      }  }
  onStepChanged(_e: { current: number }) {
    const stepTwo = this.tearsheet?.getStepValue(1);
    this.stepTwoValue = stepTwo;

    this.showAuthStep = stepTwo?.hostType !== HOST_TYPE.ALL;

    this.rebuildSteps();
  }

  rebuildSteps() {
    const steps: Step[] = [
      { label: 'Subsystem details', invalid: false },
      { label: 'Host access control', invalid: false }
    ];

    if (this.showAuthStep) {
      steps.push({ label: 'Authentication', invalid: false });
    }

    steps.push({ label: 'Review', invalid: false });

    this.steps = steps;

    if (this.tearsheet?.currentStep >= steps.length) {
      this.tearsheet.currentStep = steps.length - 1;
    }
  }

  onSubmit(payload: SubsystemPayload) {
    this.isSubmitLoading = true;
    this.lastCreatedNqn = payload.nqn;
    const stepResults: StepResult[] = [];
    const initiatorRequest: SubsystemInitiatorRequest = {
      allow_all: payload.hostType === HOST_TYPE.ALL,
      hosts: payload.hostType === HOST_TYPE.SPECIFIC ? payload.hostDchapKeyList : [],
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
