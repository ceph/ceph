import { Component, OnInit, Input, Output, EventEmitter, inject } from '@angular/core';
import { AbstractControl, FormBuilder, FormGroup, ValidationErrors, Validators } from '@angular/forms';
import { concat, of } from 'rxjs';
import { catchError, last } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CephfsDetail } from '~/app/shared/models/cephfs.model';

@Component({
  selector: 'cd-cephfs-setup-mirroring',
  templateUrl: './cephfs-setup-mirroring.component.html',
  styleUrls: ['./cephfs-setup-mirroring.component.scss'],
  standalone: false
})
export class CephfsSetupMirroringComponent implements OnInit {
  @Input() open = false;
  @Output() mirroringSetup = new EventEmitter<any>();
  @Output() cancelled = new EventEmitter<void>();

  setupForm: FormGroup;
  filesystems: { id: number; name: string }[] = [];
  isSubmitting = false;

  private cephfsService = inject(CephfsService);
  private taskWrapper = inject(TaskWrapperService);
  private fb = inject(FormBuilder);

  private validateBase64Token = (control: AbstractControl): ValidationErrors | null => {
    const value = (control.value || '').replace(/\s/g, '');
    if (!value) return null;
    if (!/^[A-Za-z0-9+/]+=*$/.test(value)) {
      return { invalidToken: true };
    }
    try {
      const decoded = atob(value);
      JSON.parse(decoded);
    } catch {
      return { invalidToken: true };
    }
    return null;
  };

  constructor() {
    this.setupForm = this.fb.group({
      filesystem: ['', Validators.required],
      token: ['', [Validators.required, this.validateBase64Token]]
    });
  }

  ngOnInit(): void {
    this.cephfsService.list().subscribe((data: CephfsDetail[]) => {
      this.filesystems = data.map((fs) => ({
        id: fs.id,
        name: fs.mdsmap?.fs_name || `fs-${fs.id}`
      }));
    });
  }

  onSetupMirroring(): void {
    if (this.setupForm.invalid) return;

    this.isSubmitting = true;
    const { filesystem, token } = this.setupForm.value;

    const apiActionsObs = concat(
      this.cephfsService.enableMirror(filesystem).pipe(
        catchError((err) => {
          err?.preventDefault?.();
          return of(null);
        })
      ),
      this.cephfsService.createBootstrapPeer(filesystem, token.replace(/\s/g, ''))
    ).pipe(last());

    const taskObs = this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('mirroring/setup', {
        fsName: filesystem
      }),
      call: apiActionsObs
    });
    taskObs.subscribe({
      error: () => {
        this.isSubmitting = false;
      },
      complete: () => {
        this.isSubmitting = false;
        this.mirroringSetup.emit({ filesystem });
      }
    });
  }

  onCancel(): void {
    this.cancelled.emit();
    this.resetState();
  }

  private resetState(): void {
    this.isSubmitting = false;
    this.setupForm.reset();
  }
}
