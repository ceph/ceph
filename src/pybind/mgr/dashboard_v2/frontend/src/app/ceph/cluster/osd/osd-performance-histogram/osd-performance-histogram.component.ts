import { Component, Input, OnChanges } from '@angular/core';

import * as _ from 'lodash';

@Component({
  selector: 'cd-osd-performance-histogram',
  templateUrl: './osd-performance-histogram.component.html',
  styleUrls: ['./osd-performance-histogram.component.scss']
})
export class OsdPerformanceHistogramComponent implements OnChanges {
  @Input() histogram: any;
  valuesStyle: any;
  last = {};

  constructor() { }

  ngOnChanges() {
    this.render();
  }

  hexdigits(v): string {
    const i = Math.floor(v * 255).toString(16);
    return i.length === 1 ? '0' + i : i;
  }

  hexcolor(r, g, b) {
    return '#' + this.hexdigits(r) + this.hexdigits(g) + this.hexdigits(b);
  }

  render() {
    if (!this.histogram) {
      return;
    }
    let sum = 0;
    let max = 0;

    _.each(this.histogram.values, (row, i) => {
      _.each(row, (col, j) => {
        let val;
        if (this.last && this.last[i] && this.last[i][j]) {
          val = col - this.last[i][j];
        } else {
          val = col;
        }
        sum += val;
        max = Math.max(max, val);
      });
    });

    this.valuesStyle = this.histogram.values.map((row, i) => {
      return row.map((col, j) => {
        const val = this.last && this.last[i] && this.last[i][j] ? col - this.last[i][j] : col;
        const g = max ? val / max : 0;
        const r = 1 - g;
        return {backgroundColor: this.hexcolor(r, g, 0)};
      });
    });

    this.last = this.histogram.values;
  }
}
