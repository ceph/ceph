import { NumberFormatterService } from '../services/number-formatter.service';

export const DECIMAL = 2;

export function getLabels(unit: string, nf: NumberFormatterService): string[] {
  switch (unit) {
    case 'B/s':
      return nf.bytesPerSecondLabels;
    case 'B':
      return nf.bytesLabels;
    case 'ms':
      return ['ms', 's'];
    default:
      return nf.unitlessLabels;
  }
}

export function getDivisor(unit: string): number {
  switch (unit) {
    case 'B/s':
    case 'B':
      return 1024;
    case 'ms':
      return 1000;
    default:
      return 1000;
  }
}

export function getDisplayUnit(
  value: number,
  baseUnit: string,
  labels: string[],
  divisor: number
): string {
  if (value <= 0) return baseUnit;

  let baseIndex = labels.findIndex((label) => label === baseUnit);
  if (baseIndex === -1) baseIndex = 0;

  const step = Math.floor(Math.log(value) / Math.log(divisor));
  const newIndex = Math.min(labels.length - 1, baseIndex + step);
  return labels[newIndex];
}

export function formatValues(
  value: number,
  baseUnit: string,
  nf: NumberFormatterService,
  decimals = DECIMAL
): string {
  const labels = getLabels(baseUnit, nf);
  const divisor = getDivisor(baseUnit);
  const displayUnit = getDisplayUnit(value, baseUnit, labels, divisor);

  return nf.formatFromTo(value, baseUnit, displayUnit, divisor, labels, decimals);
}
