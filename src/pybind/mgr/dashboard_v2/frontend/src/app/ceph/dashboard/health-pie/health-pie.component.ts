import {
  Component,
  ElementRef,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  ViewChild
} from '@angular/core';

import * as Chart from 'chart.js';
import * as _ from 'lodash';

import { ChartTooltip } from '../../../shared/models/chart-tooltip';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';

@Component({
  selector: 'cd-health-pie',
  templateUrl: './health-pie.component.html',
  styleUrls: ['./health-pie.component.scss']
})
export class HealthPieComponent implements OnChanges, OnInit {
  @ViewChild('chartCanvas') chartCanvasRef: ElementRef;
  @ViewChild('chartTooltip') chartTooltipRef: ElementRef;

  @Input() data: any;
  @Input() tooltipFn: any;
  @Output() prepareFn = new EventEmitter();

  chart: any = {
    chartType: 'doughnut',
    dataset: [
      {
        label: null,
        borderWidth: 0
      }
    ],
    options: {
      responsive: true,
      legend: { display: false },
      animation: { duration: 0 },

      tooltips: {
        enabled: false
      }
    },
    colors: [
      {
        borderColor: 'transparent'
      }
    ]
  };

  constructor(private dimlessBinary: DimlessBinaryPipe) {}

  ngOnInit() {
    // An extension to Chart.js to enable rendering some
    // text in the middle of a doughnut
    Chart.pluginService.register({
      beforeDraw: function(chart) {
        if (!chart.options.center_text) {
          return;
        }

        const width = chart.chart.width,
          height = chart.chart.height,
          ctx = chart.chart.ctx;

        ctx.restore();
        const fontSize = (height / 114).toFixed(2);
        ctx.font = fontSize + 'em sans-serif';
        ctx.textBaseline = 'middle';

        const text = chart.options.center_text,
          textX = Math.round((width - ctx.measureText(text).width) / 2),
          textY = height / 2;

        ctx.fillText(text, textX, textY);
        ctx.save();
      }
    });

    const getStyleTop = (tooltip, positionY) => {
      return positionY + tooltip.caretY - tooltip.height - 10 + 'px';
    };

    const getStyleLeft = (tooltip, positionX) => {
      return positionX + tooltip.caretX + 'px';
    };

    const getBody = (body) => {
      const bodySplit = body[0].split(': ');
      bodySplit[1] = this.dimlessBinary.transform(bodySplit[1]);
      return bodySplit.join(': ');
    };

    const chartTooltip = new ChartTooltip(
      this.chartCanvasRef,
      this.chartTooltipRef,
      getStyleLeft,
      getStyleTop,
    );
    chartTooltip.getBody = getBody;

    const self = this;
    this.chart.options.tooltips.custom = (tooltip) => {
      chartTooltip.customTooltips(tooltip);
    };

    this.prepareFn.emit([this.chart, this.data]);
  }

  ngOnChanges() {
    this.prepareFn.emit([this.chart, this.data]);
  }
}
