import { Component, OnInit } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import { FeedbackComponent } from '~/app/ceph/shared/feedback/feedback.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { DocService } from '~/app/shared/services/doc.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { AboutComponent } from '../about/about.component';

@Component({
  selector: 'cd-dashboard-help',
  templateUrl: './dashboard-help.component.html',
  styleUrls: ['./dashboard-help.component.scss']
})
export class DashboardHelpComponent implements OnInit {
  docsUrl: string;
  modalRef: NgbModalRef;
  icons = Icons;
  bsModalRef: NgbModalRef;

  constructor(private modalService: ModalService, private docService: DocService) {}

  ngOnInit() {
    this.docService.subscribeOnce('dashboard', (url: string) => {
      this.docsUrl = url;
    });
  }

  openAboutModal() {
    this.modalRef = this.modalService.show(AboutComponent, null, { size: 'lg' });
  }

  openFeedbackModal() {
    this.bsModalRef = this.modalService.show(FeedbackComponent, null, { size: 'lg' });
  }
}
