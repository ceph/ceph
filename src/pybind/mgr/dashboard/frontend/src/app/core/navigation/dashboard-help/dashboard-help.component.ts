import { Component, OnInit } from '@angular/core';

import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-dashboard-help',
  templateUrl: './dashboard-help.component.html',
  styleUrls: ['./dashboard-help.component.scss']
})
export class DashboardHelpComponent implements OnInit {

  docsUrl: string;

  constructor(private summaryService: SummaryService,
              private cephReleaseNamePipe: CephReleaseNamePipe) {}

  ngOnInit() {
    const subs = this.summaryService.summaryData$.subscribe((summary: any) => {
      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl = `http://docs.ceph.com/docs/${releaseName}/mgr/dashboard/`;
      subs.unsubscribe();
    });
  }
}
