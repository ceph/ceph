import { Component, OnDestroy, OnInit } from '@angular/core';

import { Subscription } from 'rxjs';

import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { ViewCacheStatus } from '../../../../shared/enum/view-cache-status.enum';

@Component({
  selector: 'cd-mirroring',
  templateUrl: './overview.component.html',
  styleUrls: ['./overview.component.scss']
})
export class OverviewComponent implements OnInit, OnDestroy {
  subs: Subscription;

  status: ViewCacheStatus;

  constructor(private rbdMirroringService: RbdMirroringService) {}

  ngOnInit() {
    this.subs = this.rbdMirroringService.subscribeSummary((data: any) => {
      if (!data) {
        return;
      }
      this.status = data.content_data.status;
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }
}
