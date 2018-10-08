import { Component, Input, OnChanges } from '@angular/core';

import { CdTableSelection } from '../../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-configuration-details',
  templateUrl: './configuration-details.component.html',
  styleUrls: ['./configuration-details.component.scss']
})
export class ConfigurationDetailsComponent implements OnChanges {
  @Input()
  selection: CdTableSelection;
  selectedItem: any;

  constructor() {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();
    }
  }
}
