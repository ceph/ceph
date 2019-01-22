import { Component, Input, OnChanges } from '@angular/core';

import { MgrModuleService } from '../../../../shared/api/mgr-module.service';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-mgr-module-details',
  templateUrl: './mgr-module-details.component.html',
  styleUrls: ['./mgr-module-details.component.scss']
})
export class MgrModuleDetailsComponent implements OnChanges {
  module_config: any;

  @Input()
  selection: CdTableSelection;

  constructor(private mgrModuleService: MgrModuleService) {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      const selectedItem = this.selection.first();
      this.mgrModuleService.getConfig(selectedItem.name).subscribe((resp: any) => {
        this.module_config = resp;
      });
    }
  }
}
