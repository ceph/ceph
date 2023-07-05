import { Injectable } from '@angular/core';
import { FormatterService } from './formatter.service';

@Injectable({
  providedIn: 'root'
})
export class NumberFormatterService {
  readonly bytesLabels = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
  readonly bytesPerSecondLabels = [
    'B/s',
    'KiB/s',
    'MiB/s',
    'GiB/s',
    'TiB/s',
    'PiB/s',
    'EiB/s',
    'ZiB/s',
    'YiB/s'
  ];
  readonly secondsLabels = ['ns', 'μs', 'ms', 's', 'ks', 'Ms'];
  readonly unitlessLabels = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];

  constructor(private formatter: FormatterService) {}

  formatFromTo(
    value: any,
    units: string,
    targetedUnits: string,
    factor: number,
    labels: string[]
  ): any {
    return this.formatter.formatNumberFromTo(value, units, targetedUnits, factor, labels);
  }

  formatBytesFromTo(value: any, units: string, targetedUnits: string): any {
    return this.formatFromTo(value, units, targetedUnits, 1024, this.bytesLabels);
  }

  formatBytesPerSecondFromTo(value: any, units: string, targetedUnits: string): any {
    return this.formatFromTo(value, units, targetedUnits, 1024, this.bytesPerSecondLabels);
  }

  formatSecondsFromTo(value: any, units: string, targetedUnits: string): any {
    return this.formatFromTo(value, units, targetedUnits, 1000, this.secondsLabels);
  }

  formatUnitlessFromTo(value: any, units: string, targetedUnits: string): any {
    return this.formatFromTo(value, units, targetedUnits, 1000, this.unitlessLabels);
  }
}
