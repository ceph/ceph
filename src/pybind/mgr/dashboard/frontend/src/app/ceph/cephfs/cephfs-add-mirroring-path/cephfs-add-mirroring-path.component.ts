import { Component, EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { forkJoin, of } from 'rxjs';
import { catchError, tap } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { MirroringPathsStepComponent } from './mirroring-paths-step/mirroring-paths-step.component';

export { MirroringPathsStepComponent } from './mirroring-paths-step/mirroring-paths-step.component';
export { MirroringScheduleStepComponent } from './mirroring-schedule-step/mirroring-schedule-step.component';

@Component({
  selector: 'cd-cephfs-add-mirroring-path',
  templateUrl: './cephfs-add-mirroring-path.component.html',
  styleUrls: ['./cephfs-add-mirroring-path.component.scss'],
  standalone: false
})
export class CephfsAddMirroringPathComponent {
  @Input() fsName: string;
  @Input() fsId: number;
  @Output() pathsAdded = new EventEmitter<string[]>();
  @Output() cancelled = new EventEmitter<void>();

  @ViewChild(MirroringPathsStepComponent) pathsStep!: MirroringPathsStepComponent;

  steps: Step[] = [
    { label: 'Paths', secondaryLabel: 'Optional label', invalid: false },
    { label: 'Schedule', secondaryLabel: 'Optional label', invalid: false },
    { label: 'Review', secondaryLabel: 'Optional label', invalid: false }
  ];
  label = 'Filesystem mirroring';
  title = 'Add mirroring path';
  isSubmitLoading = false;

  constructor(
    private cephfsService: CephfsService,
    private notificationService: NotificationService
  ) {}

  onSubmit(_payload: any) {
    const validPaths = this.pathsStep?.getValidPaths() || [];

    if (validPaths.length === 0) return;

    this.isSubmitLoading = true;

    const addCalls = validPaths.map((path) =>
      this.cephfsService.addMirrorDirectory(this.fsName, path).pipe(
        tap(() => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Mirroring path '${path}' added to ${this.fsName}`
          );
        }),
        catchError(() => of(null))
      )
    );

    forkJoin(addCalls).subscribe({
      next: () => {
        this.isSubmitLoading = false;
        this.pathsAdded.emit(validPaths);
      },
      error: () => {
        this.isSubmitLoading = false;
      }
    });
  }

  onClose() {
    this.cancelled.emit();
  }
}
