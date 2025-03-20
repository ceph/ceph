import { Component, Input } from '@angular/core';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

@Component({
  selector: 'cd-nfs-cluster-details',
  templateUrl: './nfs-cluster-details.component.html',
  styleUrls: ['./nfs-cluster-details.component.scss']
})
export class NfsClusterDetailsComponent {
  title = $localize`Export`;
  @Input()
  selection: CdTableSelection;
}
