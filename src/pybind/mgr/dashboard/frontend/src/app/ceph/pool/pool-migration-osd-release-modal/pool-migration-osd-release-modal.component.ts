import { Component } from '@angular/core';

import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-pool-migration-osd-release-modal',
  templateUrl: './pool-migration-osd-release-modal.component.html',
  standalone: false
})
export class PoolMigrationOsdReleaseModalComponent extends BaseModal {
  readonly upgradeUrl = '/upgrade';
  readonly docsUrl = 'https://docs.ceph.com/en/latest/cephadm/upgrade/#upgrading-ceph';
}
