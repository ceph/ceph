import { Component, Input, OnChanges, TemplateRef, ViewChild } from '@angular/core';

import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { RbdFormModel } from '../rbd-form/rbd-form.model';

@Component({
  selector: 'cd-rbd-details',
  templateUrl: './rbd-details.component.html',
  styleUrls: ['./rbd-details.component.scss']
})
export class RbdDetailsComponent implements OnChanges {
  @Input()
  selection: CdTableSelection;
  selectedItem: RbdFormModel;
  @Input()
  images: any;
  @ViewChild('poolConfigurationSourceTpl')
  poolConfigurationSourceTpl: TemplateRef<any>;

  constructor() {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();
    }
  }
}
