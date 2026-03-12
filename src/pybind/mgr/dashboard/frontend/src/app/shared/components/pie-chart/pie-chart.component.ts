import { CommonModule } from '@angular/common';
import { Component, Input, OnChanges, SimpleChanges, ChangeDetectionStrategy } from '@angular/core';
import { PieChartOptions, ChartTabularData, ChartsModule } from '@carbon/charts-angular';

@Component({
  selector: 'cd-pie-chart',
  templateUrl: './pie-chart.component.html',
  styleUrls: ['./pie-chart.component.scss'],
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [ChartsModule, CommonModule]
})
export class PieChartComponent implements OnChanges {
  @Input() data!: { group: string; value: number }[];
  @Input() title: string = '';
  @Input() legendPosition: 'top' | 'bottom' | 'left' | 'right' = 'bottom';
  @Input() height: string = '280px';

  chartData: ChartTabularData = [];
  chartOptions!: PieChartOptions;

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['data'] && this.data) {
      this.prepareData();
      this.prepareOptions();
    }
  }

  private prepareData(): void {
    this.chartData = this.data.map((d) => ({
      group: d.group,
      value: d.value
    }));
  }

  private prepareOptions(): void {
    this.chartOptions = {
      title: this.title,
      height: this.height,
      legend: {
        position: this.legendPosition
      },
      toolbar: {
        enabled: true
      },
      pie: {
        labels: {
          enabled: false
        }
      }
    };
  }
}
