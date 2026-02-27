export interface EditState {
  [rowId: string]: {
    [field: string]: string;
  };
}
