import { PipeTransform } from '@angular/core';

export interface InventoryDeviceFilter {
  label: string;
  prop: string;
  initValue: string;
  value: string;
  options: {
    value: string;
    formatValue: string;
  }[];
  pipe?: PipeTransform;
}
