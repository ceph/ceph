import { Component, Inject, Optional } from '@angular/core';
import { Router } from '@angular/router';
import { BaseModal } from 'carbon-components-angular';

interface ConnectedSubsystem {
  nqn: string;
}

@Component({
  selector: 'cd-nvmeof-gateway-group-delete-guard-modal',
  templateUrl: './nvmeof-gateway-group-delete-guard-modal.component.html',
  standalone: false
})
export class NvmeofGatewayGroupDeleteGuardModalComponent extends BaseModal {
  constructor(
    private router: Router,
    @Optional() @Inject('gatewayName') public gatewayName: string,
    @Optional() @Inject('connectedSubsystems') public connectedSubsystems: ConnectedSubsystem[] = []
  ) {
    super();
  }

  navigateToSubsystem(nqn: string): void {
    this.router.navigate(['/block/nvmeof/subsystems', nqn, 'overview'], {
      queryParams: { group: this.gatewayName }
    });
    this.closeModal();
  }
}
