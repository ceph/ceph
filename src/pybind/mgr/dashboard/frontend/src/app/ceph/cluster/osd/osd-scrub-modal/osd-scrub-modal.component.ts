import { Component, Inject, OnInit, Optional } from '@angular/core';
import { UntypedFormGroup } from '@angular/forms';

import { BaseModal } from 'carbon-components-angular';
import { forkJoin } from 'rxjs';

import { OsdService } from '~/app/shared/api/osd.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { JoinPipe } from '~/app/shared/pipes/join.pipe';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-osd-scrub-modal',
  templateUrl: './osd-scrub-modal.component.html',
  styleUrls: ['./osd-scrub-modal.component.scss']
})
export class OsdScrubModalComponent extends BaseModal implements OnInit {
  scrubForm: UntypedFormGroup;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private osdService: OsdService,
    private notificationService: NotificationService,
    private joinPipe: JoinPipe,

    @Optional() @Inject('selected') public selected: any[] = [],
    @Optional() @Inject('deep') public deep: boolean
  ) {
    super();
  }

  ngOnInit() {
    this.scrubForm = new UntypedFormGroup({});
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

        this.closeModal();
      },
      () => this.closeModal()
    );
  }
}
