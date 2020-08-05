import { Component, OnInit, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import { Icons } from '../../../shared/enum/icons.enum';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { DocService } from '../../../shared/services/doc.service';
import { ModalService } from '../../../shared/services/modal.service';
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

  constructor(
    private modalService: ModalService,
    private authStorageService: AuthStorageService,
    private docService: DocService
  ) {}

  ngOnInit() {
    this.docService.subscribeOnce('dashboard', (url: string) => {
      this.docsUrl = url;
    });
  }

  openAboutModal() {
    this.modalRef = this.modalService.show(AboutComponent, null, { size: 'lg' });
  }

  goToApiDocs() {
    const tokenInput = this.docsFormElement.nativeElement.children[0];
    tokenInput.value = this.authStorageService.getToken();
    this.docsFormElement.nativeElement.submit();
  }
}
