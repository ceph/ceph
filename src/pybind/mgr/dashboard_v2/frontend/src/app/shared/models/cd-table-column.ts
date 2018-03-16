import { TableColumn } from '@swimlane/ngx-datatable';
import { CellTemplate } from '../enum/cell-template.enum';

export interface CdTableColumn extends TableColumn {
  cellTransformation?: CellTemplate;
  isHidden?: boolean;
}
