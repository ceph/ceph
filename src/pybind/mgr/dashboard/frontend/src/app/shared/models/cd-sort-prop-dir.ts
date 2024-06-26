import { CdSortDirection } from '../enum/cd-sort-direction';

export interface CdSortPropDir {
  dir: CdSortDirection;
  prop: string | number;
}
