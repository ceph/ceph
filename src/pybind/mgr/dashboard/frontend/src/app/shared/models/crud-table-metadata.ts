import { CdTableColumn } from '~/app/shared/models/cd-table-column';

class Table {
  columns: CdTableColumn[];
  columnMode: string;
  toolHeader: boolean;
}

export class CrudMetadata {
  table: Table;
}
