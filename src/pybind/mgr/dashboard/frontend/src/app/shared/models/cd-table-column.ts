import { TableColumn, TableColumnProp } from '@swimlane/ngx-datatable';

import { CellTemplate } from '../enum/cell-template.enum';

export interface CdTableColumn extends TableColumn {
  cellTransformation?: CellTemplate;
  isHidden?: boolean;
  prop: TableColumnProp; // Enforces properties to get sortable columns
  customTemplateConfig?: any; // Custom configuration used by cell templates.
}
