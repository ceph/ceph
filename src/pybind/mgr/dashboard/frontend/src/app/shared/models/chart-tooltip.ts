import { ElementRef } from '@angular/core';

import * as _ from 'lodash';

export class ChartTooltip {
  tooltipEl: any;
  chartEl: any;
  getStyleLeft: Function;
  getStyleTop: Function;
  customColors = {
    backgroundColor: undefined,
    borderColor: undefined
  };
  checkOffset = false;

  /**
   * Creates an instance of ChartTooltip.
   * @param {ElementRef} chartCanvas Canvas Element
   * @param {ElementRef} chartTooltip Tooltip Element
   * @param {Function} getStyleLeft Function that calculates the value of Left
   * @param {Function} getStyleTop Function that calculates the value of Top
   * @memberof ChartTooltip
   */
  constructor(
    chartCanvas: ElementRef,
    chartTooltip: ElementRef,
    getStyleLeft: Function,
    getStyleTop: Function
  ) {
    this.chartEl = chartCanvas.nativeElement;
    this.getStyleLeft = getStyleLeft;
    this.getStyleTop = getStyleTop;
    this.tooltipEl = chartTooltip.nativeElement;
  }

  /**
   * Implementation of a ChartJS custom tooltip function.
   *
   * @param {any} tooltip
   * @memberof ChartTooltip
   */
  customTooltips(tooltip) {
    // Hide if no tooltip
    if (tooltip.opacity === 0) {
      this.tooltipEl.style.opacity = 0;
      return;
    }

    // Set caret Position
    this.tooltipEl.classList.remove('above', 'below', 'no-transform');
    if (tooltip.yAlign) {
      this.tooltipEl.classList.add(tooltip.yAlign);
    } else {
      this.tooltipEl.classList.add('no-transform');
    }

    // Set Text
    if (tooltip.body) {
      const titleLines = tooltip.title || [];
      const bodyLines = tooltip.body.map(bodyItem => {
        return bodyItem.lines;
      });

      let innerHtml = '<thead>';

      titleLines.forEach(title => {
        innerHtml += '<tr><th>' + this.getTitle(title) + '</th></tr>';
      });
      innerHtml += '</thead><tbody>';

      bodyLines.forEach((body, i) => {
        const colors = tooltip.labelColors[i];
        let style = 'background:' + (this.customColors.backgroundColor || colors.backgroundColor);
        style += '; border-color:' + (this.customColors.borderColor || colors.borderColor);
        style += '; border-width: 2px';
        const span = '<span class="chartjs-tooltip-key" style="' + style + '"></span>';
        innerHtml += '<tr><td nowrap>' + span + this.getBody(body) + '</td></tr>';
      });
      innerHtml += '</tbody>';

      const tableRoot = this.tooltipEl.querySelector('table');
      tableRoot.innerHTML = innerHtml;
    }

    const positionY = this.chartEl.offsetTop;
    const positionX = this.chartEl.offsetLeft;

    // Display, position, and set styles for font
    if (this.checkOffset) {
      const halfWidth = tooltip.width / 2;
      this.tooltipEl.classList.remove('transform-left');
      this.tooltipEl.classList.remove('transform-right');
      if (tooltip.caretX - halfWidth < 0) {
        this.tooltipEl.classList.add('transform-left');
      } else if (tooltip.caretX + halfWidth > this.chartEl.width) {
        this.tooltipEl.classList.add('transform-right');
      }
    }

    this.tooltipEl.style.left = this.getStyleLeft(tooltip, positionX);
    this.tooltipEl.style.top = this.getStyleTop(tooltip, positionY);

    this.tooltipEl.style.opacity = 1;
    this.tooltipEl.style.fontFamily = tooltip._fontFamily;
    this.tooltipEl.style.fontSize = tooltip.fontSize;
    this.tooltipEl.style.fontStyle = tooltip._fontStyle;
    this.tooltipEl.style.padding = tooltip.yPadding + 'px ' + tooltip.xPadding + 'px';
  }

  getBody(body) {
    return body;
  }

  getTitle(title) {
    return title;
  }
}
