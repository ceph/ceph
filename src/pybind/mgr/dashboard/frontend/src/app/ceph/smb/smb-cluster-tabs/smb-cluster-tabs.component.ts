import { Component, Input } from '@angular/core';
import { SMBCluster } from '../smb.model';

@Component({
  selector: 'cd-smb-cluster-tabs',
  templateUrl: './smb-cluster-tabs.component.html',
  styleUrls: ['./smb-cluster-tabs.component.scss']
})
export class SmbClusterTabsComponent {
  @Input()
  selection: SMBCluster;
}
