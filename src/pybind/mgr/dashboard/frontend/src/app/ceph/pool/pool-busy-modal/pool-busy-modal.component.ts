import { Component, Inject, Optional } from '@angular/core';

import { BaseModal } from 'carbon-components-angular';

import { PoolAction, PoolBusyReason } from '../pool';

/** Informational only: tells the user to wait for what is currently running on the pool. */
@Component({
  selector: 'cd-pool-busy-modal',
  templateUrl: './pool-busy-modal.component.html',
  standalone: false
})
export class PoolBusyModalComponent extends BaseModal {
  readonly title: string;
  readonly description: string;

  constructor(
    @Optional() @Inject('reason') public reason: PoolBusyReason,
    @Optional() @Inject('taskDescription') public taskDescription: string,
    @Optional() @Inject('action') public action: PoolAction = PoolAction.MIGRATE
  ) {
    super();
    this.title = this.buildTitle();
    this.description = this.buildDescription();
  }

  private buildTitle(): string {
    switch (this.action) {
      case PoolAction.EDIT:
        return $localize`Cannot edit pool`;
      case PoolAction.DELETE:
        return $localize`Cannot delete pool`;
      default:
        return $localize`Cannot migrate pool`;
    }
  }

  private actionLabel(): string {
    switch (this.action) {
      case PoolAction.EDIT:
        return $localize`editing`;
      case PoolAction.DELETE:
        return $localize`deletion`;
      default:
        return $localize`migration`;
    }
  }

  private buildDescription(): string {
    const action = this.actionLabel();
    switch (this.reason) {
      case PoolBusyReason.MIGRATING:
        return $localize`Pool ${action} is currently unavailable because this pool is already part of an ongoing migration. Wait for it to finish and try again.`;
      case PoolBusyReason.PG_NUM_CHANGE:
        return $localize`Pool ${action} is currently unavailable because the number of PGs (pg_num) of this pool is still being adjusted toward its target. Wait for pg_num to match pg_num_target and try again.`;
      case PoolBusyReason.PGP_NUM_CHANGE:
        return $localize`Pool ${action} is currently unavailable because the PG placement count (pgp_num) is still converging to its target. The pool's PGs may already look healthy (for example active+clean); wait until pgp_num matches pgp_num_target and try again.`;
      case PoolBusyReason.PG_CHANGE:
        return $localize`Pool ${action} is currently unavailable because both the number of PGs (pg_num) and the PG placement count (pgp_num) are still being adjusted toward their targets. Wait for both to finish and try again.`;
      default:
        return $localize`Pool ${action} is currently unavailable because another operation is in progress on this pool (${this.taskDescription}). Wait for it to finish and try again.`;
    }
  }
}
