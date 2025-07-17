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
    targetedUnits: string = '',
    factor: number,
    labels: string[],
    decimals: number = 1
  ): any {
    return this.formatter.formatNumberFromTo(value, units, targetedUnits, factor, labels, decimals);
  }

  formatBytesFromTo(value: any, units: string, targetedUnits: string, decimals: number = 1): any {
    return this.formatFromTo(value, units, targetedUnits, 1024, this.bytesLabels, decimals);
  }

  formatBytesPerSecondFromTo(
    value: any,
    units: string,
    targetedUnits: string,
    decimals: number = 1
  ): any {
    return this.formatFromTo(
      value,
      units,
      targetedUnits,
      1024,
      this.bytesPerSecondLabels,
      decimals
    );
  }

  formatSecondsFromTo(value: any, units: string, targetedUnits: string, decimals: number = 1): any {
    return this.formatFromTo(value, units, targetedUnits, 1000, this.secondsLabels, decimals);
  }

  formatUnitlessFromTo(
    value: any,
    units: string,
    targetedUnits: string = '',
    decimals: number = 1
  ): any {
    return this.formatFromTo(value, units, targetedUnits, 1000, this.unitlessLabels, decimals);
  }

  convertBytes(bytes: number, decimals: number): { convertedValue: number, unit: string } {
  if (bytes === 0) return { convertedValue: 0, unit: 'B' };
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  const convertedValue = parseFloat((bytes / Math.pow(k, i)).toFixed(decimals));
  return { convertedValue, unit: sizes[i] };
}

// Similar for B/s and ms (or reuse with different suffix)
convertBytesPerSecond(bytes: number, decimals: number) {
  const result = this.convertBytes(bytes, decimals);
  result.unit += '/s';
  return result;
}

convertSeconds(ms: number, decimals: number) {
  // If ms >= 1000, convert to sec
  if (ms >= 1000) {
    return { convertedValue: parseFloat((ms / 1000).toFixed(decimals)), unit: 's' };
  }
  return { convertedValue: parseFloat(ms.toFixed(decimals)), unit: 'ms' };
}
}
