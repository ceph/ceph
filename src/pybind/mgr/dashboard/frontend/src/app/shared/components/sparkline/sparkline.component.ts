import {
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges,
  ViewChild
} from '@angular/core';

import { ChartTooltip } from '../../models/chart-tooltip';
import { DimlessBinaryPipe } from '../../pipes/dimless-binary.pipe';

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

  @Input()
  data: any;
  @Input()
  style = {
    height: '30px',
    width: '100px'
  };
  @Input()
  isBinary: boolean;

  public colors: Array<any> = [
    {
      backgroundColor: 'rgba(40,140,234,0.2)',
      borderColor: 'rgba(40,140,234,1)',
      pointBackgroundColor: 'rgba(40,140,234,1)',
      pointBorderColor: '#fff',
      pointHoverBackgroundColor: '#fff',
      pointHoverBorderColor: 'rgba(40,140,234,0.8)'
    }
  ];

  options: Record<string, any> = {
    animation: {
      duration: 0
    },
    responsive: true,
    maintainAspectRatio: false,
    legend: {
      display: false
    },
    elements: {
      line: {
        borderWidth: 1
      }
    },
    tooltips: {
      enabled: false,
      mode: 'index',
      intersect: false,
      custom: undefined,
      callbacks: {
        label: (tooltipItem: any) => {
          if (this.isBinary) {
            return this.dimlessBinaryPipe.transform(tooltipItem.yLabel);
          } else {
            return tooltipItem.yLabel;
          }
        },
        title: () => ''
      }
    },
    scales: {
      yAxes: [
        {
          display: false
        }
      ],
      xAxes: [
        {
          display: false
        }
      ]
    }
  };

  public datasets: Array<any> = [
    {
      data: []
    }
  ];

  public labels: Array<any> = [];

  constructor(private dimlessBinaryPipe: DimlessBinaryPipe) {}

  ngOnInit() {
    const getStyleTop = (tooltip: any) => {
      return tooltip.caretY - tooltip.height - tooltip.yPadding - 5 + 'px';
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
      backgroundColor: this.colors[0].pointBackgroundColor,
      borderColor: this.colors[0].pointBorderColor
    };

    this.options.tooltips.custom = (tooltip: any) => {
      chartTooltip.customTooltips(tooltip);
    };
  }

  ngOnChanges(changes: SimpleChanges) {
    this.datasets[0].data = changes['data'].currentValue;
    this.labels = [...Array(changes['data'].currentValue.length)];
  }
}
