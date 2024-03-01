import { Component, Input } from '@angular/core';
import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-rgw-sync-data-info',
  templateUrl: './rgw-sync-data-info.component.html',
  styleUrls: ['./rgw-sync-data-info.component.scss']
})
export class RgwSyncDataInfoComponent {
  icons = Icons;

  @Input()
  zone: any = {};

  constructor() {}
}
