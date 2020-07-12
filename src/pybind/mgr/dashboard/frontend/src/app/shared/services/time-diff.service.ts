import { Injectable } from '@angular/core';

import * as _ from 'lodash';

@Injectable({
  providedIn: 'root'
})
export class TimeDiffService {
  calculateDuration(startDate: Date, endDate: Date): string {
    const startTime = +startDate;
    const endTime = +endDate;
    const duration = this.getDuration(Math.abs(startTime - endTime));
    if (startTime > endTime) {
      return '-' + duration;
    }
    return duration;
  }

  /**
   * Get the duration in the format '[Nd] [Nh] [Nm]', e.g. '2d 1h 15m'.
   * @param ms The time in milliseconds.
   * @return The duration. An empty string is returned if the duration is
   *   less than a minute.
   */
  private getDuration(ms: number): string {
    const date = new Date(ms);
    const h = date.getUTCHours();
    const m = date.getUTCMinutes();
    const d = Math.floor(ms / (24 * 3600 * 1000));

    const format = (n: number, s: string) => (n ? n + s : n);
    return [format(d, 'd'), format(h, 'h'), format(m, 'm')].filter((x) => x).join(' ');
  }

  calculateDate(date: Date, duration: string, reverse?: boolean): Date {
    const time = +date;
    if (_.isNaN(time)) {
      return undefined;
    }
    const diff = this.getDurationMs(duration) * (reverse ? -1 : 1);
    return new Date(time + diff);
  }

  private getDurationMs(duration: string): number {
    const d = this.getNumbersFromString(duration, 'd');
    const h = this.getNumbersFromString(duration, 'h');
    const m = this.getNumbersFromString(duration, 'm');
    return ((d * 24 + h) * 60 + m) * 60000;
  }

  private getNumbersFromString(duration: string, prefix: string): number {
    const match = duration.match(new RegExp(`[0-9 ]+${prefix}`, 'i'));
    return match ? parseInt(match[0], 10) : 0;
  }
}
