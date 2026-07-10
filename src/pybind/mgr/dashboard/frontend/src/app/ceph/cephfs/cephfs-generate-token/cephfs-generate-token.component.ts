import { Component, OnInit, OnDestroy, Input, Output, EventEmitter, inject } from '@angular/core';
import {
  AbstractControl,
  FormBuilder,
  ValidationErrors,
  ValidatorFn,
  Validators
} from '@angular/forms';
import { merge, Observable, OperatorFunction, Subject } from 'rxjs';
import { debounceTime, finalize, map, switchMap, takeUntil, tap } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ClusterService } from '~/app/shared/api/cluster.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import {
  BootstrapTokenResponse,
  CephfsDetail,
  CLIENT_PREFIX,
  MAX_TYPEAHEAD_SUGGESTIONS,
  VALID_USERNAME_PATTERN
} from '~/app/shared/models/cephfs.model';
import { CephAuthUser } from '~/app/shared/models/cluster.model';

@Component({
  selector: 'cd-cephfs-generate-token',
  templateUrl: './cephfs-generate-token.component.html',
  standalone: false,
  styleUrls: ['./cephfs-generate-token.component.scss']
})
export class CephfsGenerateTokenComponent implements OnInit, OnDestroy {
  @Input() open = false;
  @Output() tokenGenerated = new EventEmitter<string>();
  @Output() cancelled = new EventEmitter<void>();

  filesystems: { id: number; name: string }[] = [];
  filteredUsers: string[] = [];
  isGenerating = false;
  generatedToken = '';
  usernameFocus$ = new Subject<string>();

  private allClientUsers: CephAuthUser[] = [];
  private destroy$ = new Subject<void>();

  private cephfsService = inject(CephfsService);
  private clusterService = inject(ClusterService);
  private taskWrapper = inject(TaskWrapperService);
  private fb = inject(FormBuilder);

  private noClientPrefix: ValidatorFn = (control: AbstractControl): ValidationErrors | null => {
    const value = (control.value ?? '').toString().trim();
    if (!value) return null;
    return value.startsWith(CLIENT_PREFIX) ? { forbiddenClientPrefix: true } : null;
  };

  private validUsername = (control: AbstractControl): ValidationErrors | null => {
    const value = (control.value ?? '').toString();
    if (!value) return null;
    if (VALID_USERNAME_PATTERN.test(value)) return { invalidChars: true };
    return null;
  };

  tokenForm = this.fb.group({
    filesystem: ['', Validators.required],
    username: ['', [Validators.required, this.noClientPrefix, this.validUsername]],
    sitename: ['', Validators.required]
  });

  ngOnInit(): void {
    this.loadFilesystems();
    this.loadExistingUsers();
    this.tokenForm.controls['filesystem'].valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.updateFilteredUsers();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  searchUsername: OperatorFunction<string, readonly string[]> = (text$: Observable<string>) =>
    merge(text$, this.usernameFocus$).pipe(
      debounceTime(200),
      map((term) =>
        this.filteredUsers
          .filter((u) => !term || u.toLowerCase().includes(term.toLowerCase()))
          .slice(0, MAX_TYPEAHEAD_SUGGESTIONS)
      )
    );

  get submitText(): string {
    return this.isGenerating ? $localize`Generating...` : $localize`Generate token`;
  }

  onGenerateToken(): void {
    if (this.tokenForm.invalid) return;

    this.isGenerating = true;
    const { filesystem, username, sitename } = this.tokenForm.value;
    const fullEntity = `${CLIENT_PREFIX}${username}`;

    const apiActions = this.cephfsService.enableMirror(filesystem).pipe(
      switchMap(() =>
        this.cephfsService.createBootstrapToken(filesystem, fullEntity, sitename).pipe(
          tap((res: BootstrapTokenResponse) => {
            const token = res?.token || res?.data || '';
            if (!token) {
              throw new Error('Bootstrap token missing from API response');
            }
            this.generatedToken = token;
          })
        )
      )
    );

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('mirroring/token/create', {
          fsName: filesystem,
          clientName: fullEntity,
          siteName: sitename
        }),
        call: apiActions
      })
      .pipe(finalize(() => (this.isGenerating = false)))
      .subscribe({
        complete: () => {
          if (this.generatedToken) {
            this.tokenGenerated.emit(this.generatedToken);
          }
        },
        error: () => {
          this.generatedToken = '';
        }
      });
  }

  onCancel(): void {
    this.cancelled.emit();
    this.generatedToken = '';
    this.isGenerating = false;
    this.tokenForm.reset();
  }

  private loadFilesystems(): void {
    this.cephfsService.list().subscribe((data: CephfsDetail[]) => {
      this.filesystems = data.map((fs) => ({
        id: fs.id,
        name: fs.mdsmap?.fs_name || `fs-${fs.id}`
      }));
    });
  }

  private loadExistingUsers(): void {
    this.clusterService.listUser().subscribe({
      next: (users: CephAuthUser[]) => {
        this.allClientUsers = (users || []).filter((u) => {
          const entity = String(u.entity || u['user_entity'] || '');
          return entity.startsWith(CLIENT_PREFIX);
        });
        this.updateFilteredUsers();
      },
      error: () => {
        this.allClientUsers = [];
        this.filteredUsers = [];
      }
    });
  }

  private updateFilteredUsers(): void {
    const selectedFs = this.tokenForm.controls['filesystem'].value;
    if (!selectedFs) {
      this.filteredUsers = [];
      return;
    }
    this.filteredUsers = this.allClientUsers
      .filter((u) => {
        const mdsCaps = u.caps?.mds || '';
        return mdsCaps.includes(`fsname=${selectedFs}`);
      })
      .map((u) => String(u.entity || u['user_entity'] || '').slice(CLIENT_PREFIX.length));
  }
}
