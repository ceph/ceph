import { Component, OnInit, ViewChild } from '@angular/core';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';

import { Icons } from '../../../shared/enum/icons.enum';
import { DocService } from '../../../shared/services/doc.service';
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

  constructor(private modalService: BsModalService, private docService: DocService) {}

  ngOnInit() {
    this.docService.subscribeOnce('dashboard', (url: string) => {
      this.docsUrl = url;
    });
  }

  openAboutModal() {
    this.modalRef = this.modalService.show(AboutComponent);
    this.modalRef.setClass('modal-lg');
  }

  goToApiDocs() {
    this.docsFormElement.nativeElement.submit();
  }
}
