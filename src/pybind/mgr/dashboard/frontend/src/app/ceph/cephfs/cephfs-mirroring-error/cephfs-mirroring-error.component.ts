import { Component, ViewEncapsulation } from '@angular/core';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { CEPHFS_MIRRORING_URL } from '~/app/shared/constants/cephfs.constant';

@Component({
  selector: 'cd-cephfs-mirroring-error',
  templateUrl: './cephfs-mirroring-error.component.html',
  styleUrls: ['./cephfs-mirroring-error.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class CephfsMirroringErrorComponent {
  constructor(private mgrModuleService: MgrModuleService) {}

  enableModule(): void {
    this.mgrModuleService.updateModuleState(
      'mirroring',
      false,
      null,
      CEPHFS_MIRRORING_URL,
      $localize`CephFS Mirroring module enabled`,
      false,
      $localize`Enabling CephFS Mirroring module. Reconnecting, please wait ...`
    );
  }
}
