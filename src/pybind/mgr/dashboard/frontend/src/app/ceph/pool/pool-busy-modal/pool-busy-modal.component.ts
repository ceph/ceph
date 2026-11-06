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
  readonly reasons = PoolBusyReason;
  readonly actions = PoolAction;

  constructor(
    @Optional() @Inject('reason') public reason: PoolBusyReason,
    @Optional() @Inject('taskDescription') public taskDescription: string,
    @Optional() @Inject('action') public action: PoolAction = PoolAction.MIGRATE
  ) {
    super();
  }
}
