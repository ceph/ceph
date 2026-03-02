import { Component, ViewEncapsulation } from '@angular/core';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';

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
      'cephfs/mirroring',
      $localize`CephFS Mirroring module enabled`,
      false,
      $localize`Enabling CephFS Mirroring. Reconnecting, please wait ...`
    );
  }
}
