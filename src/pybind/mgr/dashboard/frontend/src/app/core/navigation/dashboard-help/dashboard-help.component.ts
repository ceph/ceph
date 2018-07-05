import { Component, OnInit } from '@angular/core';

import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { SummaryService } from '../../../shared/services/summary.service';
import { AboutComponent } from '../about/about.component';

@Component({
  selector: 'cd-dashboard-help',
  templateUrl: './dashboard-help.component.html',
  styleUrls: ['./dashboard-help.component.scss']
})
export class DashboardHelpComponent implements OnInit {
  docsUrl: string;
  modalRef: BsModalRef;

  constructor(
    private summaryService: SummaryService,
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private modalService: BsModalService
  ) {}

  ngOnInit() {
    const subs = this.summaryService.subscribe((summary: any) => {
      if (!summary) {
        return;
      }

      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl = `http://docs.ceph.com/docs/${releaseName}/mgr/dashboard/`;

      setTimeout(() => {
        subs.unsubscribe();
      }, 0);
    });
  }

  openAboutModal() {
    this.modalRef = this.modalService.show(AboutComponent);
  }
}
