import { CdTableColumnFiltersChange } from '~/app/shared/models/cd-table-column-filters-change';

export interface DevicesSelectionChangeEvent extends CdTableColumnFiltersChange {
  type: string;
}
