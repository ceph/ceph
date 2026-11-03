import { ChangeDetectionStrategy, Component, input } from '@angular/core';
import { LayoutModule, TooltipModule } from 'carbon-components-angular';

import { OverviewStat } from '~/app/shared/models/overview';

@Component({
  selector: 'cd-overview-stats',
  imports: [LayoutModule, TooltipModule],
  templateUrl: './overview-stats.component.html',
  styleUrl: './overview-stats.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewStatsComponent {
  readonly stats = input.required<OverviewStat[]>();
}
