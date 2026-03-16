import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  inject,
  Input,
  ViewEncapsulation
} from '@angular/core';
import { GridModule, TooltipModule, SkeletonModule, LayoutModule } from 'carbon-components-angular';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { MeterChartComponent, MeterChartOptions } from '@carbon/charts-angular';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { AreaChartComponent } from '~/app/shared/components/area-chart/area-chart.component';
import { ComponentsModule } from '~/app/shared/components/components.module';

const CHART_HEIGHT = '45px';

type TrendPoint = {
  timestamp: Date;
  values: { Used: number };
};

@Component({
  selector: 'cd-overview-storage-card',
  imports: [
    GridModule,
    ProductiveCardComponent,
    MeterChartComponent,
    TooltipModule,
    SkeletonModule,
    LayoutModule,
    AreaChartComponent,
    ComponentsModule
  ],
  standalone: true,
  templateUrl: './overview-storage-card.component.html',
  styleUrl: './overview-storage-card.component.scss',
  encapsulation: ViewEncapsulation.None,
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewStorageCardComponent {
  private readonly formatterService = inject(FormatterService);
  private readonly cdr = inject(ChangeDetectorRef);

  @Input()
  set totalCapacity(value: number) {
    const [totalValue, totalUnit] = this.formatterService.formatToBinary(value, true);
    if (Number.isNaN(totalValue)) return;
    this.totalRaw = totalValue;
    this.totalRawUnit = totalUnit;
    this.updateChartOptions();
  }

  @Input()
  set usedCapacity(value: number) {
    const [usedValue, usedUnit] = this.formatterService.formatToBinary(value, true);
    if (Number.isNaN(usedValue)) return;
    this.usedRaw = usedValue;
    this.usedRawUnit = usedUnit;
    this.updateChartOptions();
  }

  @Input() consumptionTrendData: TrendPoint[] = [];
  @Input() averageDailyConsumption = '';
  @Input() estimatedTimeUntilFull = '';
  @Input() breakdownData: { group: string; value: number }[] = [];
  @Input() isBreakdownLoaded = false;

  totalRaw: number | null = null;
  usedRaw: number | null = null;
  totalRawUnit = '';
  usedRawUnit = '';

  options: MeterChartOptions = {
    height: CHART_HEIGHT,
    meter: {
      proportional: {
        total: null,
        unit: '',
        breakdownFormatter: (_e) => null,
        totalFormatter: (_e) => null
      }
    },
    toolbar: {
      enabled: false
    },
    color: {
      pairing: {
        option: 2
      }
    }
  };

  private updateChartOptions() {
    if (
      this.totalRaw === null ||
      this.usedRaw === null ||
      !this.totalRawUnit ||
      !this.usedRawUnit
    ) {
      return;
    }
    this.options = {
      ...this.options,
      meter: {
        ...this.options.meter,
        proportional: {
          ...this.options.meter.proportional,
          total: this.totalRaw,
          unit: this.totalRawUnit
        }
      },
      tooltip: {
        valueFormatter: (value) => `${value.toLocaleString()} ${this.usedRawUnit}`
      }
    };

    this.markForCheck();
  }

  private markForCheck() {
    this.cdr.markForCheck();
  }
}
