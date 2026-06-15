import { Component, OnInit, Output, EventEmitter, inject } from '@angular/core';
import { Validators } from '@angular/forms';
import { concat, forkJoin, of } from 'rxjs';
import { catchError, finalize, last } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CephfsDetail, CephfsMirroringSetupEvent, Daemon } from '~/app/shared/models/cephfs.model';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdValidators } from '~/app/shared/forms/cd-validators';

@Component({
  selector: 'cd-cephfs-setup-mirroring',
  templateUrl: './cephfs-setup-mirroring.component.html',
  styleUrls: ['./cephfs-setup-mirroring.component.scss'],
  standalone: false
})
export class CephfsSetupMirroringComponent extends CdForm implements OnInit {
  @Output() mirroringSetup = new EventEmitter<CephfsMirroringSetupEvent>();

  setupForm: CdFormGroup;
  filesystems: { id: number; name: string }[] = [];
  isSubmitting = false;

  private cephfsService = inject(CephfsService);
  private cephServiceService = inject(CephServiceService);
  private taskWrapper = inject(TaskWrapperService);
  private fb = inject(CdFormBuilder);

  constructor() {
    super();
    this.setupForm = this.fb.group({
      filesystem: ['', Validators.required],
      token: ['', [Validators.required, CdValidators.base64Json()]]
    });
  }

  ngOnInit(): void {
    forkJoin({
      filesystems: this.cephfsService.list(),
      daemons: this.cephfsService.listDaemonStatus().pipe(catchError(() => of([] as Daemon[])))
    }).subscribe(({ filesystems, daemons }) => {
      const mirroredNames = this.getMirroredFilesystem(daemons, filesystems as CephfsDetail[]);
      this.filesystems = (filesystems as CephfsDetail[])
        .map((fs) => ({
          id: fs.id,
          name: fs.mdsmap?.fs_name || `fs-${fs.id}`
        }))
        .filter((fs) => !mirroredNames.has(fs.name));
    });
  }

  get submitText(): string {
    return this.isSubmitting ? $localize`Setting up...` : $localize`Setup mirroring`;
  }

  onSetupMirroring(): void {
    if (this.setupForm.invalid) return;

    this.isSubmitting = true;
    const { filesystem, token } = this.setupForm.value;

    const apiActionsObs = concat(
      this.cephServiceService.create({
        service_type: 'cephfs-mirror'
      }),
      this.cephfsService.enableMirror(filesystem),
      this.cephfsService.createBootstrapPeer(filesystem, token.replace(/\s/g, ''))
    ).pipe(last());

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('cephfs/mirroring/setup', {
          fsName: filesystem
        }),
        call: apiActionsObs
      })
      .pipe(finalize(() => (this.isSubmitting = false)))
      .subscribe({
        complete: () => {
          this.mirroringSetup.emit({ filesystem });
        },
        error: () => {
          this.setupForm.setErrors({ cdSubmitButton: true });
        }
      });
  }

  closeModal(): void {
    this.isSubmitting = false;
    this.setupForm.reset();
    super.closeModal();
  }

  private getMirroredFilesystem(daemons: Daemon[], filesystems: CephfsDetail[]): Set<string> {
    const names = new Set<string>();

    for (const daemon of daemons || []) {
      for (const fs of daemon.filesystems || []) {
        if (fs.peers?.length) {
          names.add(fs.name);
        }
      }
    }

    for (const fs of filesystems || []) {
      const fsName = fs.mdsmap?.fs_name || `fs-${fs.id}`;
      const peers = fs.mirror_info?.peers ?? fs.cephfs?.mirror_info?.peers;
      if (peers && Object.keys(peers).length > 0) {
        names.add(fsName);
      }
    }

    return names;
  }
}
