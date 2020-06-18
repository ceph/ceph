import { Component, Input } from '@angular/core';

import { ViewCacheStatus } from '../../enum/view-cache-status.enum';

@Component({
  selector: 'cd-view-cache',
  templateUrl: './view-cache.component.html',
  styleUrls: ['./view-cache.component.scss']
})
export class ViewCacheComponent {
  @Input()
  status: ViewCacheStatus;
  @Input()
  statusFor: string;
  vcs = ViewCacheStatus;
}
