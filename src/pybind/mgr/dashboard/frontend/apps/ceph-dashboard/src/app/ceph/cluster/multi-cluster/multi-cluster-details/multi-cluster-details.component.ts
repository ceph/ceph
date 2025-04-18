import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-multi-cluster-details',
  templateUrl: './multi-cluster-details.component.html',
  styleUrls: ['./multi-cluster-details.component.scss']
})
export class MultiClusterDetailsComponent {
  @Input()
  permissions: Permissions;

  @Input()
  selection: any;

  get selectedClusterFsid(): string {
    return this.selection !== undefined ? this.selection['name'] : null;
  }
}
