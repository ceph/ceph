import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { RouterModule } from '@angular/router';
import { LayoutModule, LayerModule, LinkModule, TilesModule } from 'carbon-components-angular';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { ComponentsModule } from '~/app/shared/components/components.module';

@Component({
  selector: 'cd-nvmeof-setup-cards',
  templateUrl: './nvmeof-setup-cards.component.html',
  styleUrl: './nvmeof-setup-cards.component.scss',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    LayoutModule,
    LayerModule,
    TilesModule,
    LinkModule,
    ProductiveCardComponent,
    ComponentsModule
  ]
})
export class NvmeofSetupCardsComponent {}
