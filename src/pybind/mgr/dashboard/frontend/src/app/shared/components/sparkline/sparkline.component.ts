import {
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges,
  ViewChild
} from '@angular/core';

import { BaseChartDirective } from 'ng2-charts';
import { ChartTooltip } from '~/app/shared/models/chart-tooltip';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';

@Component({
  selector: 'cd-sparkline',
  templateUrl: './sparkline.component.html',
  styleUrls: ['./sparkline.component.scss']
})
export class SparklineComponent implements OnInit, OnChanges {
  @ViewChild('sparkCanvas', { static: true })
  chartCanvasRef: ElementRef;
  @ViewChild('sparkTooltip', { static: true })
  chartTooltipRef: ElementRef;
  @ViewChild(BaseChartDirective) chart: BaseChartDirective;

  @Input()
  data: any;
  @Input()
  style = {
    height: '30px',
    width: '100px'
  };
  @Input()
  isBinary: boolean;

  options: Record<string, any> = {
    plugins: {
      legend: {
        display: false
      },
      tooltip: {
        enabled: false,
        mode: 'index',
        intersect: false,
        custom: undefined,
        callbacks: {
          label: (tooltipItem: any) => {
            if (!tooltipItem.parsed) return;
            if (this.isBinary) {
              return this.dimlessBinaryPipe.transform(tooltipItem.parsed.y);
            } else {
              return tooltipItem.parsed.y;
            }
          },
          title: () => ''
        }
      }
    },
    animation: {
      duration: 0
    },
    responsive: true,
    maintainAspectRatio: false,
    elements: {
      line: {
        borderWidth: 1
      }
    },
    scales: {
      y: {
        display: false
      },
      x: {
        display: false
      }
    }
  };

  public datasets: Array<any> = [
    {
      data: [],
      backgroundColor: 'rgba(40,140,234,0.2)',
      borderColor: 'rgba(40,140,234,1)',
      pointBackgroundColor: 'rgba(40,140,234,1)',
      pointBorderColor: '#fff',
      pointHoverBackgroundColor: '#fff',
      pointHoverBorderColor: 'rgba(40,140,234,0.8)'
    }
  ];

  public labels: Array<any> = [];

  chartData: {
    datasets: any[];
    labels: any[];
  } = {
    datasets: this.datasets,
    labels: this.labels
  };

  constructor(private dimlessBinaryPipe: DimlessBinaryPipe) {}

  ngOnInit() {
    const getStyleTop = (tooltip: any) => {
      return tooltip.caretY - tooltip.height - 6 - 5 + 'px';
    };

    const getStyleLeft = (tooltip: any, positionX: number) => {
      return positionX + tooltip.caretX + 'px';
    };

    const chartTooltip = new ChartTooltip(
      this.chartCanvasRef,
      this.chartTooltipRef,
      getStyleLeft,
      getStyleTop
    );

    chartTooltip.customColors = {
      backgroundColor: this.datasets[0].pointBackgroundColor,
      borderColor: this.datasets[0].pointBorderColor
    };

    this.options.plugins.tooltip.external = (tooltip: any) => {
      chartTooltip.customTooltips(tooltip);
    };
  }

  ngOnChanges(changes: SimpleChanges) {
    this.chartData.datasets[0].data = changes['data'].currentValue;
    this.chartData.labels = [...Array(changes['data'].currentValue.length).fill('')];
    if (this.chart) {
      this.chart.chart.update();
    }
  }
}
