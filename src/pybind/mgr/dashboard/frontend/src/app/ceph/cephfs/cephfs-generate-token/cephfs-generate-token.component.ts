import { Component, OnInit, Input, Output, EventEmitter, inject } from '@angular/core';
import { AbstractControl, FormBuilder, ValidationErrors, Validators } from '@angular/forms';
import { concat, merge, Observable, of, OperatorFunction, Subject } from 'rxjs';
import { catchError, debounceTime, last, map, tap } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ClusterService } from '~/app/shared/api/cluster.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import {
  BootstrapTokenResponse,
  CephfsDetail,
  CLIENT_PREFIX,
  MAX_TYPEAHEAD_SUGGESTIONS,
  USERNAME_PATTERN
} from '~/app/shared/models/cephfs.model';
import { CephAuthUser } from '~/app/shared/models/cluster.model';

@Component({
  selector: 'cd-cephfs-generate-token',
  templateUrl: './cephfs-generate-token.component.html',
  standalone: false,
  styleUrls: ['./cephfs-generate-token.component.scss']
})
export class CephfsGenerateTokenComponent implements OnInit {
  @Input() open = false;
  @Output() tokenGenerated = new EventEmitter<string>();
  @Output() cancelled = new EventEmitter<void>();

  filesystems: { id: number; name: string }[] = [];
  existingUsers: string[] = [];
  filteredUsers: string[] = [];
  isGenerating = false;
  generatedToken = '';
  usernameFocus$ = new Subject<string>();

  private allClientUsers: CephAuthUser[] = [];

  private cephfsService = inject(CephfsService);
  private clusterService = inject(ClusterService);
  private taskWrapper = inject(TaskWrapperService);
  private fb = inject(FormBuilder);

  private validUsername = (control: AbstractControl): ValidationErrors | null => {
    const value = (control.value ?? '').toString();
    if (!value) return null;
    if (USERNAME_PATTERN.test(value)) return { invalidChars: true };
    return null;
  };

  tokenForm = this.fb.group({
    filesystem: ['', Validators.required],
    username: ['', [Validators.required, this.validUsername]],
    sitename: ['']
  });

  ngOnInit(): void {
    this.loadFilesystems();
    this.loadExistingUsers();
    this.tokenForm.controls['filesystem'].valueChanges.subscribe(() => {
      this.updateFilteredUsers();
    });
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

  onGenerateToken(): void {
    if (this.tokenForm.invalid) return;

    this.isGenerating = true;
    const { filesystem, username, sitename } = this.tokenForm.value;
    const fullEntity = `${CLIENT_PREFIX}${username}`;
    const site = sitename || filesystem;

    const isExistingUser = this.existingUsers.includes(username);

    const apiActions = concat(
      this.cephfsService.enableMirror(filesystem).pipe(catchError(() => of(null))),
      isExistingUser
        ? of(null)
        : this.clusterService
            .createUser({
              user_entity: fullEntity,
              capabilities: [
                { entity: 'mds', cap: 'allow *' },
                { entity: 'mgr', cap: 'allow *' },
                { entity: 'mon', cap: 'allow *' },
                { entity: 'osd', cap: 'allow *' }
              ]
            })
            .pipe(catchError(() => of(null))),
      isExistingUser
        ? of(null)
        : this.cephfsService.setAuth(filesystem, username, ['/', 'rwps'], false),
      this.cephfsService.createBootstrapToken(filesystem, fullEntity, site).pipe(
        tap((res: BootstrapTokenResponse) => {
          this.generatedToken = res?.token || res?.data || '';
        })
      )
    ).pipe(last());

    const finishHandler = () => {
      this.isGenerating = false;
      this.tokenGenerated.emit(this.generatedToken);
    };

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('mirroring/token/create', {
          fsName: filesystem,
          clientName: fullEntity,
          siteName: site
        }),
        call: apiActions
      })
      .subscribe({ error: finishHandler, complete: finishHandler });
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
        this.existingUsers = this.allClientUsers.map((u) =>
          String(u.entity || u['user_entity'] || '').slice(CLIENT_PREFIX.length)
        );
        this.updateFilteredUsers();
      },
      error: () => {
        this.allClientUsers = [];
        this.existingUsers = [];
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
