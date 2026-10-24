import { ChangeDetectionStrategy, Component, input, ViewEncapsulation } from '@angular/core';
import { GridModule, TooltipModule } from 'carbon-components-angular';

import { AreaChartComponent } from '~/app/shared/components/area-chart/area-chart.component';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { OverviewStatsComponent } from '~/app/shared/components/overview-stats/overview-stats.component';
import { SavingsCardVM } from '~/app/shared/models/overview';

@Component({
  selector: 'cd-overview-storage-savings',
  standalone: true,
  imports: [
    ComponentsModule,
    GridModule,
    TooltipModule,
    AreaChartComponent,
    OverviewStatsComponent
  ],
  templateUrl: './overview-storage-savings.component.html',
  styleUrl: './overview-storage-savings.component.scss',
  encapsulation: ViewEncapsulation.None,
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewStorageSavingsComponent {
  readonly savings = input.required<SavingsCardVM>();
}
