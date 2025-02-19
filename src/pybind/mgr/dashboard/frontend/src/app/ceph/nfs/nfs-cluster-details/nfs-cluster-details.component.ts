import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-nfs-cluster-details',
  templateUrl: './nfs-cluster-details.component.html',
  styleUrls: ['./nfs-cluster-details.component.scss']
})
export class NfsClusterDetailsComponent {
  title = 'Export';
  @Input()
  selection: any;
}
