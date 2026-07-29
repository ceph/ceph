import { Component, Input } from '@angular/core';
import { ICON_TYPE } from '~/app/shared/enum/icons.enum'

@Component({
  selector: 'cd-rgw-sync-metadata-info',
  templateUrl: './rgw-sync-metadata-info.component.html',
  styleUrls: ['./rgw-sync-metadata-info.component.scss'],
  standalone: false
})
export class RgwSyncMetadataInfoComponent {
  icons = ICON_TYPE;
  align = 'top';
  @Input()
  metadataSyncInfo: any = {};

  constructor() {}
}
