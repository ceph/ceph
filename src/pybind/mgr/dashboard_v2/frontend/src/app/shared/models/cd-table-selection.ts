export class CdTableSelection {
  selected: any[] = [];

  get hasSelection(): boolean {
    return this.selected.length > 0;
  }

  get hasSingleSelection(): boolean {
    return this.selected.length === 1;
  }

  get hasMultiSelection(): boolean {
    return this.selected.length > 1;
  }

  first() {
    return this.hasSelection ? this.selected[0] : null;
  }
}
