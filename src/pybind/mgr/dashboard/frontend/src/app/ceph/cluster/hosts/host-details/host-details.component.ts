import { Component, Input, OnChanges } from '@angular/core';

import { CdTableSelection } from '../../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-host-details',
  templateUrl: './host-details.component.html',
  styleUrls: ['./host-details.component.scss']
})
export class HostDetailsComponent implements OnChanges {
  @Input()
  selection: CdTableSelection;
  host: any;

  constructor() {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.host = this.selection.first();
    }
  }
}
