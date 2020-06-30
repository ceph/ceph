import { Component, Input, OnChanges } from '@angular/core';

import * as _ from 'lodash';

@Component({
  selector: 'cd-osd-performance-histogram',
  templateUrl: './osd-performance-histogram.component.html',
  styleUrls: ['./osd-performance-histogram.component.scss']
})
export class OsdPerformanceHistogramComponent implements OnChanges {
  @Input()
  histogram: any;
  valuesStyle: any;
  last = {};

  ngOnChanges() {
    this.render();
  }

  hexdigits(v: number): string {
    const i = Math.floor(v * 255).toString(16);
    return i.length === 1 ? '0' + i : i;
  }

  hexcolor(r: number, g: number, b: number) {
    return '#' + this.hexdigits(r) + this.hexdigits(g) + this.hexdigits(b);
  }

  render() {
    if (!this.histogram) {
      return;
    }
    let max = 0;

    _.each(this.histogram.values, (row, i) => {
      _.each(row, (col, j) => {
        let val;
        if (this.last && this.last[i] && this.last[i][j]) {
          val = col - this.last[i][j];
        } else {
          val = col;
        }
        max = Math.max(max, val);
      });
    });

    this.valuesStyle = this.histogram.values.map((row: any, i: number) => {
      return row.map((col: any, j: number) => {
        const val = this.last && this.last[i] && this.last[i][j] ? col - this.last[i][j] : col;
        const g = max ? val / max : 0;
        const r = 1 - g;
        return { backgroundColor: this.hexcolor(r, g, 0) };
      });
    });

    this.last = this.histogram.values;
  }
}
