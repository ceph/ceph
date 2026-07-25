import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { RouterModule } from '@angular/router';
import { LayoutModule, LayerModule, LinkModule, TilesModule } from 'carbon-components-angular';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { SetupStepCardComponent } from '~/app/shared/components/setup-step-card/setup-step-card.component';

interface SetupCardConfig {
  title: string;
  description: string;
  successMessage: string;
  infoMessage: string;
}

@Component({
  selector: 'cd-nvmeof-setup-cards',
  templateUrl: './nvmeof-setup-cards.component.html',
  styleUrls: ['./nvmeof-setup-cards.component.scss'],
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    LayoutModule,
    LayerModule,
    TilesModule,
    LinkModule,
    ComponentsModule,
    SetupStepCardComponent
  ]
})
export class NvmeofSetupCardsComponent {
  @Input() hasGatewayGroups = false;
  @Input() hasSubsystems = false;
  @Input() hasNamespaces = false;
  @Input() isAllConfigured = false;
  @Output() viewStatus = new EventEmitter<void>();

  cards: Record<'gateway' | 'subsystem' | 'namespace', SetupCardConfig> = {
    gateway: {
      title: $localize`Gateway Group`,
      description: $localize`Group your NVMe gateway nodes together to provide high availability and load distribution for your storage targets.`,
      successMessage: $localize`Gateway group configured successfully.`,
      infoMessage: $localize`No gateway groups configured for this cluster yet.`
    },
    subsystem: {
      title: $localize`Create Subsystems`,
      description: $localize`Create NVMe subsystems that define storage targets. Configure security, listeners, and host access permissions.`,
      successMessage: $localize`Subsystem configured successfully.`,
      infoMessage: $localize`No subsystem configured for this cluster yet.`
    },
    namespace: {
      title: $localize`Create Namespaces`,
      description: $localize`Create storage namespaces backed by Ceph block images. Map existing volumes or create new ones for your applications.`,
      successMessage: $localize`Namespaces mapped successfully.`,
      infoMessage: $localize`No namespace available or mapped yet.`
    }
  };

  onViewStatus(event: Event): void {
    event.preventDefault();
    this.viewStatus.emit();
  }
}
