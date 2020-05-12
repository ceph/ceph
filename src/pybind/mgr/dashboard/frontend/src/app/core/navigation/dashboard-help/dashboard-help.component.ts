import { Component, OnInit, ViewChild } from '@angular/core';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';

import { Icons } from '../../../shared/enum/icons.enum';
import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { AboutComponent } from '../about/about.component';

@Component({
  selector: 'cd-dashboard-help',
  templateUrl: './dashboard-help.component.html',
  styleUrls: ['./dashboard-help.component.scss']
})
export class DashboardHelpComponent implements OnInit {
  @ViewChild('docsForm', { static: true })
  docsFormElement: any;
  docsUrl: string;
  modalRef: BsModalRef;
  icons = Icons;

  constructor(
    private summaryService: SummaryService,
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private modalService: BsModalService,
    private authStorageService: AuthStorageService
  ) {}

  ngOnInit() {
    this.summaryService.subscribeOnce((summary) => {
      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl = `http://docs.ceph.com/docs/${releaseName}/mgr/dashboard/`;
    });
  }

  openAboutModal() {
    this.modalRef = this.modalService.show(AboutComponent);
    this.modalRef.setClass('modal-lg');
  }

  goToApiDocs() {
    const tokenInput = this.docsFormElement.nativeElement.children[0];
    tokenInput.value = this.authStorageService.getToken();
    this.docsFormElement.nativeElement.submit();
  }
}
