export class CdTableSelection {
  private _selected: any[] = [];
  hasMultiSelection: boolean;
  hasSingleSelection: boolean;
  hasSelection: boolean;

  constructor(rows?: any[]) {
    if (rows) {
      this._selected = rows;
    }
    this.update();
  }

  /**
   * Recalculate the variables based on the current number
   * of selected rows.
   */
  private update() {
    this.hasSelection = this._selected.length > 0;
    this.hasSingleSelection = this._selected.length === 1;
    this.hasMultiSelection = this._selected.length > 1;
  }

  set selected(selection: any[]) {
    this._selected = selection;
    this.update();
  }

  get selected() {
    return this._selected;
  }

  add(row: any) {
    this._selected.push(row);
    this.update();
  }

  /**
   * Get the first selected row.
   * @return {any | null}
   */
  first() {
    return this.hasSelection ? this._selected[0] : null;
  }
}
