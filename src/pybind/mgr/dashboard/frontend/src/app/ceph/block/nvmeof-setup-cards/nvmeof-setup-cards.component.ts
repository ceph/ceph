import { CommonModule } from '@angular/common';
import { Component, Input, ViewEncapsulation } from '@angular/core';
import { RouterModule } from '@angular/router';
import { LayoutModule, LayerModule, LinkModule, TilesModule } from 'carbon-components-angular';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { SetupStepCardComponent } from '~/app/shared/components/setup-step-card/setup-step-card.component';

@Component({
  selector: 'cd-nvmeof-setup-cards',
  templateUrl: './nvmeof-setup-cards.component.html',
  styleUrls: ['./nvmeof-setup-cards.component.scss'],
  standalone: true,
  encapsulation: ViewEncapsulation.None,
  imports: [
    CommonModule,
    RouterModule,
    LayoutModule,
    LayerModule,
    TilesModule,
    LinkModule,
    ProductiveCardComponent,
    SetupStepCardComponent
  ]
})
export class NvmeofSetupCardsComponent {
  @Input() hasGatewayGroups = false;
  @Input() hasSubsystems = false;
  @Input() hasNamespaces = false;
  @Input() isAllConfigured = false;

  readonly gatewayPendingMessage = $localize`No gateway configured yet.`;

  readonly cards = {
    gateway: {
      title: $localize`Create Gateway groups`,
      description: $localize`Group NVMe gateway nodes to enable high availability and load balancing for storage targets.`,
      successMessage: $localize`Gateway group configured successfully.`,
      infoMessage: $localize`No gateway groups configured for this cluster yet.`
    },
    subsystem: {
      title: $localize`Create Subsystems`,
      description: $localize`Define storage targets by creating NVMe subsystems and configuring security, listeners, and host access.`,
      successMessage: $localize`Subsystem configured successfully.`,
      infoMessage: $localize`No subsystem configured for this cluster yet.`
    },
    namespace: {
      title: $localize`Create Namespaces`,
      description: $localize`Create storage namespaces backed by Ceph block images. This completes your NVMe over Fabrics setup.`,
      successMessage: $localize`Namespaces mapped successfully.`,
      infoMessage: $localize`No namespace allocated or mapped yet.`
    }
  };
}
