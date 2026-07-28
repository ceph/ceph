import { Pipe, PipeTransform } from '@angular/core';

export interface OverviewGridColumnNumbers {
  sm: number;
  md: number;
  lg: number;
}

/* Pipe to transform the number of columns into responsive grid column numbers for overview resource page */
@Pipe({
  name: 'overviewGridColumnNumbers',
  standalone: false
})
export class OverviewGridColumnNumbersPipe implements PipeTransform {
  private readonly maxGridColumns = 16;

  transform(columns: number): OverviewGridColumnNumbers {
    const roundedColumns = Math.floor(columns);
    const normalizedColumns = Math.min(Math.max(roundedColumns, 1), this.maxGridColumns);

    return {
      sm: 4,
      // md grid has 8 columns - cap at 2 columns per row beyond single-column layouts
      md: normalizedColumns === 1 ? 8 : 4,
      lg: Math.max(1, Math.floor(this.maxGridColumns / normalizedColumns))
    };
  }
}
