import { Component, Input, OnChanges } from '@angular/core';
import { NvmeofSubsystem } from '~/app/shared/models/nvmeof';
import { Permissions } from '~/app/shared/models/permissions';

@Component({
  selector: 'cd-nvmeof-subsystems-details',
  templateUrl: './nvmeof-subsystems-details.component.html',
  styleUrls: ['./nvmeof-subsystems-details.component.scss']
})
export class NvmeofSubsystemsDetailsComponent implements OnChanges {
  @Input()
  selection: NvmeofSubsystem;
  @Input()
  group: string;
  @Input()
  permissions: Permissions;

  selectedItem: any;
  data: any;
  subsystemNQN: string;

  ngOnChanges() {
    if (this.selection) {
      this.selectedItem = this.selection;
      this.subsystemNQN = this.selectedItem.nqn;

      this.data = {};
      this.data[$localize`Serial Number`] = this.selectedItem.serial_number;
      this.data[$localize`Model Number`] = this.selectedItem.model_number;
      this.data[$localize`Minimum Controller Identifier`] = this.selectedItem.min_cntlid;
      this.data[$localize`Maximum Controller Identifier`] = this.selectedItem.max_cntlid;
      this.data[$localize`Subsystem Type`] = this.selectedItem.subtype;
    }
  }
}
