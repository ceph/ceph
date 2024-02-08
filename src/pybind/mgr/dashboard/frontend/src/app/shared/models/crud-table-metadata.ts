import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableAction } from './cd-table-action';

class Table {
  columns: CdTableColumn[];
  columnMode: string;
  toolHeader: boolean;
  selectionType: string;
}

export class CrudMetadata {
  table: Table;
  permissions: string[];
  actions: CdTableAction[];
  forms: any;
  columnKey: string;
  resource: string;
}
