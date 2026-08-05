import { Component, OnInit } from '@angular/core';

import { Icons } from '~/app/shared/enum/icons.enum';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
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
  configOptPermission: Permission;

  constructor(
    private docService: DocService,
    private modalCdsService: ModalCdsService,
    private authStorageService: AuthStorageService
  ) {
    this.configOptPermission = this.authStorageService.getPermissions().configOpt;
  }

  ngOnInit() {
    this.docService.subscribeOnce('dashboard', (url: string) => {
      this.docsUrl = url;
    });
  }

  openAboutModal() {
    this.modalCdsService.show(AboutComponent);
  }

  openFeedbackModal() {
    this.modalCdsService.show(FeedbackComponent);
  }
}
