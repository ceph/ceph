import { Component, OnInit } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { OsdService } from '../../../../shared/api/osd.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { NotificationService } from '../../../../shared/services/notification.service';

@Component({
  selector: 'cd-osd-scrub-modal',
  templateUrl: './osd-scrub-modal.component.html',
  styleUrls: ['./osd-scrub-modal.component.scss']
})
export class OsdScrubModalComponent implements OnInit {
  deep: boolean;
  selected = [];
  scrubForm: FormGroup;

  constructor(
    public bsModalRef: BsModalRef,
    private osdService: OsdService,
    private notificationService: NotificationService,
    private i18n: I18n
  ) {}

  ngOnInit() {
    this.scrubForm = new FormGroup({});
  }

  scrub() {
    const id = this.selected[0].id;

    this.osdService.scrub(id, this.deep).subscribe(
      () => {
        const operation = this.deep ? 'Deep scrub' : 'Scrub';

        this.notificationService.show(
          NotificationType.success,
          this.i18n('{{operation}} was initialized in the following OSD: {{id}}', {
            operation: operation,
            id: id
          })
        );

        this.bsModalRef.hide();
      },
      () => {
        this.bsModalRef.hide();
      }
    );
  }
}
