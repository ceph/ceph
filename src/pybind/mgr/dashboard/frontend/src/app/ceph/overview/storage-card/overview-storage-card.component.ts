import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  inject,
  Input,
  ViewEncapsulation
} from '@angular/core';
import {
  GridModule,
  TooltipModule,
  SkeletonModule,
  LayoutModule,
  TagModule,
  ButtonModule
} from 'carbon-components-angular';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { MeterChartComponent, MeterChartOptions } from '@carbon/charts-angular';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { AreaChartComponent } from '~/app/shared/components/area-chart/area-chart.component';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { BreakdownChartData, CapacityThreshold, TrendPoint } from '~/app/shared/models/overview';
import { EmptyStateComponent } from '~/app/shared/components/empty-state/empty-state.component';
import { RouterModule } from '@angular/router';

const CHART_HEIGHT = '45px';

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
    ComponentsModule,
    TagModule,
    EmptyStateComponent,
    ButtonModule,
    RouterModule
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

  @Input() storageEmptyState: boolean = false;
  @Input() prometheusEmptyState: boolean = false;

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
  @Input() breakdownData: BreakdownChartData[] = [];
  @Input() isBreakdownLoaded = false;
  @Input() threshold: CapacityThreshold;

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

    const totalInUsedUnit = this.formatterService.convertToUnit(
      this.totalRaw,
      this.totalRawUnit,
      this.usedRawUnit,
      1
    );
    this.options = {
      ...this.options,
      meter: {
        ...this.options.meter,
        proportional: {
          ...this.options.meter.proportional,
          total: totalInUsedUnit,
          unit: this.usedRawUnit
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
