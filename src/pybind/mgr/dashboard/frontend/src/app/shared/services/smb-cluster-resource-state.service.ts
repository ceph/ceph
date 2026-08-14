import { Injectable } from '@angular/core';
import { ReplaySubject } from 'rxjs';

import { SmbService } from '~/app/shared/api/smb.service';
import { SMBCluster } from '../../ceph/smb/smb.model';

@Injectable()
export class SmbClusterResourceStateService {
  private clusterSource = new ReplaySubject<SMBCluster | null>(1);

  readonly cluster$ = this.clusterSource.asObservable();

  constructor(private smbService: SmbService) {}

  load(clusterIdRoute: string): void {
    if (!clusterIdRoute) {
      this.clusterSource.next(null);
      return;
    }

    try {
      const clusterId = decodeURIComponent(clusterIdRoute);
      this.smbService.getCluster(clusterId).subscribe({
        next: (cluster: SMBCluster) => this.clusterSource.next(cluster),
        error: () => this.clusterSource.next(null)
      });
    } catch {
      this.clusterSource.next(null);
    }
  }
}
