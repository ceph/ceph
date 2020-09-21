import { Component, OnInit } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { forkJoin } from 'rxjs';

import { OsdService } from '../../../../shared/api/osd.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { JoinPipe } from '../../../../shared/pipes/join.pipe';
import { NotificationService } from '../../../../shared/services/notification.service';

@Component({
  selector: 'cd-osd-scrub-modal',
  templateUrl: './osd-scrub-modal.component.html',
  styleUrls: ['./osd-scrub-modal.component.scss']
})
export class OsdScrubModalComponent implements OnInit {
  deep: boolean;
  scrubForm: FormGroup;
  selected: any[] = [];

  constructor(
    public activeModal: NgbActiveModal,
    private osdService: OsdService,
    private notificationService: NotificationService,
    private joinPipe: JoinPipe
  ) {}

  ngOnInit() {
    this.scrubForm = new FormGroup({});
  }

  scrub() {
    forkJoin(this.selected.map((id: any) => this.osdService.scrub(id, this.deep))).subscribe(
      () => {
        const operation = this.deep ? 'Deep scrub' : 'Scrub';

        this.notificationService.show(
          NotificationType.success,
          $localize`${operation} was initialized in the following OSD(s): ${this.joinPipe.transform(
            this.selected
          )}`
        );

        this.activeModal.close();
      },
      () => this.activeModal.close()
    );
  }
}
