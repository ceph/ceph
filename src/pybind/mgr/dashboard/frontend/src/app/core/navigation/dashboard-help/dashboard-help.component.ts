import { Component, OnInit, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

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
  @ViewChild('docsForm', { static: true })
  docsFormElement: any;
  docsUrl: string;
  modalRef: NgbModalRef;
  icons = Icons;

  constructor(private modalService: ModalService, private docService: DocService) {}

  ngOnInit() {
    this.docService.subscribeOnce('dashboard', (url: string) => {
      this.docsUrl = url;
    });
  }

  openAboutModal() {
    this.modalRef = this.modalService.show(AboutComponent, null, { size: 'lg' });
  }

  goToApiDocs() {
    this.docsFormElement.nativeElement.submit();
  }
}
