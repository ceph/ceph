import { Component, Input } from '@angular/core';
import { IconSize } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-rgw-sync-data-info',
  templateUrl: './rgw-sync-data-info.component.html',
  styleUrls: ['./rgw-sync-data-info.component.scss'],
  standalone: false
})
export class RgwSyncDataInfoComponent {
  iconSize = IconSize;
  align = 'top';
  @Input()
  zone: any = {};
  constructor() {}
}
