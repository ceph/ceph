import { Component, OnInit } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import { Icons } from '~/app/shared/enum/icons.enum';
import { DocService } from '~/app/shared/services/doc.service';

import { AboutComponent } from '../about/about.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { FeedbackComponent } from '~/app/ceph/shared/feedback/feedback.component';
import { ModalService } from '~/app/shared/services/modal.service';

@Component({
  selector: 'cd-dashboard-help',
  templateUrl: './dashboard-help.component.html',
  styleUrls: ['./dashboard-help.component.scss']
})
export class DashboardHelpComponent implements OnInit {
  docsUrl: string;
  icons = Icons;
  bsModalRef: NgbModalRef;

  constructor(
    private docService: DocService,
    private modalCdsService: ModalCdsService,
    private modalService: ModalService
  ) {}

  ngOnInit() {
    this.docService.subscribeOnce('dashboard', (url: string) => {
      this.docsUrl = url;
    });
  }

  openAboutModal() {
    this.modalCdsService.show(AboutComponent);
  }

  openFeedbackModal() {
    this.bsModalRef = this.modalService.show(FeedbackComponent, null, { size: 'lg' });
  }
}
