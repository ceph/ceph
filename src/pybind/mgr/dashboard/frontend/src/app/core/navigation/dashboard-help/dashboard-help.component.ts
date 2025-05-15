import { Component, OnInit } from '@angular/core';

import { Icons } from '~/app/shared/enum/icons.enum';
import { DocService } from '~/app/shared/services/doc.service';

import { AboutComponent } from '../about/about.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { FeedbackComponent } from '~/app/ceph/shared/feedback/feedback.component';

@Component({
  selector: 'cd-dashboard-help',
  templateUrl: './dashboard-help.component.html',
  styleUrls: ['./dashboard-help.component.scss']
})
export class DashboardHelpComponent implements OnInit {
  docsUrl: string;
  icons = Icons;

  constructor(
    private docService: DocService,
    private cdsModalService: ModalCdsService
  ) {}

  ngOnInit() {
    this.docService.subscribeOnce('dashboard', (url: string) => {
      this.docsUrl = url;
    });
  }

  openAboutModal() {
    this.cdsModalService.show(AboutComponent);
  }

  openFeedbackModal() {
    this.cdsModalService.show(FeedbackComponent);
  }
}
