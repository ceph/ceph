import { Component, Input, OnChanges } from '@angular/core';

import { MgrModuleService } from '../../../../shared/api/mgr-module.service';

@Component({
  selector: 'cd-mgr-module-details',
  templateUrl: './mgr-module-details.component.html',
  styleUrls: ['./mgr-module-details.component.scss']
})
export class MgrModuleDetailsComponent implements OnChanges {
  module_config: any;

  @Input()
  selection: any;

  constructor(private mgrModuleService: MgrModuleService) {}

  ngOnChanges() {
    if (this.selection) {
      this.mgrModuleService.getConfig(this.selection.name).subscribe((resp: any) => {
        this.module_config = resp;
      });
    }
  }
}
