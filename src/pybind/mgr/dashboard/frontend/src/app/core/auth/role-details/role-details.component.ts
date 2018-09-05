import { Component, Input, OnChanges } from '@angular/core';

import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-role-details',
  templateUrl: './role-details.component.html',
  styleUrls: ['./role-details.component.scss']
})
export class RoleDetailsComponent implements OnChanges {
  @Input()
  selection: CdTableSelection;
  @Input()
  scopes: Array<string>;
  selectedItem: any;

  constructor() {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();
    }
  }
}
