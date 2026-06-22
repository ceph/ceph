import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { Step } from 'carbon-components-angular';
import { forkJoin, of } from 'rxjs';
import { catchError, tap } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { MirroringPathsStepComponent } from './mirroring-paths-step/mirroring-paths-step.component';
import { MirroringPathSelection } from './mirroring-path.model';

@Component({
  selector: 'cd-cephfs-add-mirroring-path',
  templateUrl: './cephfs-add-mirroring-path.component.html',
  styleUrls: ['./cephfs-add-mirroring-path.component.scss'],
  standalone: false
})
export class CephfsAddMirroringPathComponent implements OnInit {
  @Input() fsName: string;
  @Input() fsId: number;
  @Output() pathsAdded = new EventEmitter<string[]>();
  @Output() cancelled = new EventEmitter<void>();

  @ViewChild('pathsStep') pathsStep!: MirroringPathsStepComponent;

  steps: Step[] = [];
  label: string;
  title: string;
  isSubmitLoading = false;
  selectedPaths: MirroringPathSelection[] = [];

  constructor(
    private cephfsService: CephfsService,
    private notificationService: NotificationService
  ) {}

  ngOnInit() {
    this.label = $localize`Filesystem mirroring`;
    this.title = $localize`Add mirroring path`;
    this.steps = [
      { label: $localize`Paths`, secondaryLabel: $localize`Optional label`, invalid: false },
      { label: $localize`Schedule`, secondaryLabel: $localize`Optional label`, invalid: false }
    ];
  }

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

  onPathsChanged(paths: MirroringPathSelection[]): void {
    this.selectedPaths = [...paths];
  }

  onStepChanged(event: { current: number }): void {
    if (event.current === 1) {
      this.selectedPaths = this.pathsStep?.getPathSelections() ?? [];
    }
  }
}
