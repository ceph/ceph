import { Component, Input } from '@angular/core';
import { IconSize, Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-rgw-sync-primary-zone',
  templateUrl: './rgw-sync-primary-zone.component.html',
  styleUrls: ['./rgw-sync-primary-zone.component.scss'],
  standalone: false
})
export class RgwSyncPrimaryZoneComponent {
  icons = Icons; // Keep for FontAwesome icons (down, cubes)
  iconSize = IconSize;

  @Input()
  realm: string;

  @Input()
  zonegroup: string;

  @Input()
  zone: string;

  constructor() {}
}
