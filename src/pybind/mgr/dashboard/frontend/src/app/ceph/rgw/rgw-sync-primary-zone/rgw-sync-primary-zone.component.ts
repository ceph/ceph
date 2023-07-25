import { Component, Input } from '@angular/core';
import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-rgw-sync-primary-zone',
  templateUrl: './rgw-sync-primary-zone.component.html',
  styleUrls: ['./rgw-sync-primary-zone.component.scss']
})
export class RgwSyncPrimaryZoneComponent {
  icons = Icons;

  @Input()
  realm: string;

  @Input()
  zonegroup: string;

  @Input()
  zone: string;

  constructor() {}
}
