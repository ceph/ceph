import { Component, Input, OnChanges } from '@angular/core';

import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-rbd-details',
  templateUrl: './rbd-details.component.html',
  styleUrls: ['./rbd-details.component.scss']
})
export class RbdDetailsComponent implements OnChanges {

  @Input() selection: CdTableSelection;
  selectedItem: any;

  constructor() { }

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();
    }
  }
}
