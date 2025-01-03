import { Component, Input } from '@angular/core';
import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-rgw-sync-metadata-info',
  templateUrl: './rgw-sync-metadata-info.component.html',
  styleUrls: ['./rgw-sync-metadata-info.component.scss']
})
export class RgwSyncMetadataInfoComponent {
  icons = Icons;

  @Input()
  metadataSyncInfo: any = {};

  constructor() {}
}
