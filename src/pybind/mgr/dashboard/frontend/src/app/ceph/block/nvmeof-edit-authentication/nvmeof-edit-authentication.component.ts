import {
  AfterViewInit,
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Inject,
  OnInit,
  Output,
  ViewChild
} from '@angular/core';
import { FormGroup } from '@angular/forms';
import { forkJoin } from 'rxjs';
import { finalize } from 'rxjs/operators';

import { BaseModal } from 'carbon-components-angular';
import { AuthKeyUpdate, NvmeofService } from '~/app/shared/api/nvmeof.service';
import {
  AUTHENTICATION,
  HostStepType,
  NvmeofSubsystem,
  NvmeofSubsystemInitiator,
  getSubsystemAuthStatus,
  NvmeofSubsystemAuthType
} from '~/app/shared/models/nvmeof';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NvmeofSubsystemsStepThreeComponent } from '../nvmeof-subsystems-form/nvmeof-subsystem-step-3/nvmeof-subsystem-step-3.component';

export const SUBSYSTEM_NQN_TOKEN = 'subsystemNQN';
export const GROUP_NAME_TOKEN = 'groupName';

@Component({
  selector: 'cd-nvmeof-edit-authentication',
  templateUrl: './nvmeof-edit-authentication.component.html',
  styleUrls: ['./nvmeof-edit-authentication.component.scss'],
  standalone: false
})
export class NvmeofEditAuthenticationComponent extends BaseModal implements OnInit, AfterViewInit {
  @ViewChild(NvmeofSubsystemsStepThreeComponent)
  authStep!: NvmeofSubsystemsStepThreeComponent;

  @Output() closeChange = new EventEmitter<void>();

  isSubmitLoading = false;
  stepTwoValue: HostStepType | null = null;
  subsystemHasDhchapKey = false;
  initialAuthType: AUTHENTICATION = AUTHENTICATION.Unidirectional;
  showAuthAlert = false;

  readonly steps = [{ label: $localize`Authentication`, invalid: false }];
  readonly title = $localize`Edit authentication`;
  readonly description = $localize`Configure authentication to verify the identity of connecting hosts and protect the subsystem from unauthorized access.`;

  constructor(
    @Inject(SUBSYSTEM_NQN_TOKEN) public subsystemNQN: string,
    @Inject(GROUP_NAME_TOKEN) public groupName: string,
    private nvmeofService: NvmeofService,
    private notificationService: NotificationService,
    private modalCdsService: ModalCdsService,
    private cdr: ChangeDetectorRef
  ) {
    super();
  }

  ngOnInit() {
    forkJoin({
      subsystem: this.nvmeofService.getSubsystem(this.subsystemNQN, this.groupName),
      initiators: this.nvmeofService.getInitiators(this.subsystemNQN, this.groupName)
    }).subscribe(({ subsystem, initiators }) => {
      const sub = subsystem as NvmeofSubsystem;
      this.subsystemHasDhchapKey = sub.has_dhchap_key;
      this.initialAuthType = this.deriveAuthType(sub, initiators);
      this.stepTwoValue = this.buildStepTwoValue(initiators);
    });
  }

  /**
   * @ViewChild is guaranteed to be set by ngAfterViewInit.
   * Subscribing here ensures the authType form control exists before we attach
   * the valueChanges listener that drives showAuthAlert.
   */
  ngAfterViewInit() {
    this.authStep.formGroup.get('authType')?.valueChanges.subscribe((authType: AUTHENTICATION) => {
      this.showAuthAlert = this.subsystemHasDhchapKey && authType === AUTHENTICATION.Unidirectional;
      this.cdr.markForCheck();
    });
  }

  /**
   * Maps the current subsystem auth status string to the AUTHENTICATION enum
   * so the form radio pre-selects the correct option on open.
   */
  private deriveAuthType(subsystem: NvmeofSubsystem, initiators: unknown): AUTHENTICATION {
    const status = getSubsystemAuthStatus(
      subsystem,
      initiators as NvmeofSubsystemInitiator[] | { hosts?: NvmeofSubsystemInitiator[] }
    );
    switch (status) {
      case NvmeofSubsystemAuthType.BIDIRECTIONAL:
        return AUTHENTICATION.Bidirectional;
      case NvmeofSubsystemAuthType.UNIDIRECTIONAL:
        return AUTHENTICATION.Unidirectional;
      default:
        return AUTHENTICATION.Unidirectional;
    }
  }

  private buildStepTwoValue(initiators: unknown): HostStepType {
    const hosts = this.extractHostsFromResponse(initiators);
    return {
      addedHosts: hosts.map((h) => h.nqn),
      hostname: '',
      hostType: 'specific'
    };
  }

  /**
   * Handles both response shapes from getInitiators:
   * - direct array: `NvmeofSubsystemInitiator[]`
   * - object wrapper: `{ hosts: NvmeofSubsystemInitiator[] }`
   */
  private extractHostsFromResponse(response: unknown): NvmeofSubsystemInitiator[] {
    if (!response) {
      return [];
    }
    if (Array.isArray(response)) {
      return response;
    }
    const hostWrapper = response as { hosts?: NvmeofSubsystemInitiator[] };
    return hostWrapper.hosts ?? [];
  }

  onSubmit() {
    const form = this.getValidatedForm();
    if (!form) {
      return;
    }

    this.isSubmitLoading = true;

    const update = this.buildAuthenticationUpdate(form);

    this.nvmeofService
      .updateAuthenticationKey(this.subsystemNQN, this.groupName, update)
      .pipe(
        finalize(() => {
          this.isSubmitLoading = false;
        })
      )
      .subscribe({
        error: () => {
          form.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Authentication updated`,
            $localize`Authentication settings for subsystem ${this.subsystemNQN} have been saved.`
          );
          this.closeChange.emit();
          this.modalCdsService.dismissAll();
        }
      });
  }

  private getValidatedForm(): FormGroup | null {
    const form = this.authStep?.formGroup;
    return form && !form.invalid ? form : null;
  }

  private buildAuthenticationUpdate(form: FormGroup): AuthKeyUpdate {
    return {
      authType: form.get('authType')?.value,
      subsystemKey: form.get('subsystemDchapKey')?.value,
      hostKeyList: form.get('hostDchapKeyList')?.value
    };
  }
}
