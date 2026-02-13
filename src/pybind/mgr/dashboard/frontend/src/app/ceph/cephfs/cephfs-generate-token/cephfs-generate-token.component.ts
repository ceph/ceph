import {
  Component,
  Input,
  OnInit,
  OnChanges,
  SimpleChanges,
  ChangeDetectorRef,
  inject
} from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { map, tap } from 'rxjs/operators';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { HealthService } from '~/app/shared/api/health.service';
import { FilesystemRow, MirroringEntityRow } from '~/app/shared/models/cephfs.model';

interface TokenResponse {
  token?: string;
  data?: string;
}

@Component({
  selector: 'cd-cephfs-generate-token',
  templateUrl: './cephfs-generate-token.component.html',
  standalone: false,
  styleUrls: ['./cephfs-generate-token.component.scss']
})
export class CephfsGenerateTokenComponent implements OnInit, OnChanges {
  @Input() selectedFilesystem: FilesystemRow | null = null;
  @Input() selectedEntity: MirroringEntityRow | string | null = null;

  filesystemName: string = '';
  entityName: string = '';
  siteName = new FormControl('', Validators.required);
  isGenerating: boolean = false;
  generatedToken: string = '';
  clusterId: string = '';
  showEnvironmentAlert: boolean = true;
  showSuccessAlert: boolean = true;
  showWarningAlert: boolean = true;
  showInfoAlert: boolean = true;

  private cephfsService = inject(CephfsService);
  private taskWrapperService = inject(TaskWrapperService);
  private healthService = inject(HealthService);
  private cdr = inject(ChangeDetectorRef);

  ngOnInit(): void {
    this.healthService.getHealthSnapshot().subscribe((snapshot: any) => {
      this.clusterId = snapshot?.fsid || '';
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['selectedFilesystem'] && this.selectedFilesystem) {
      this.filesystemName = this.selectedFilesystem?.name || '';
      this.generatedToken = '';
      this.siteName.setValue('');
      this.siteName.markAsUntouched();
      this.cdr.detectChanges();
    }
    if (changes['selectedEntity'] && this.selectedEntity) {
      if (typeof this.selectedEntity === 'string') {
        this.entityName = this.selectedEntity;
      } else {
        this.entityName = this.selectedEntity?.entity || '';
      }
      this.generatedToken = '';
      this.siteName.setValue('');
      this.siteName.markAsUntouched();
      this.cdr.detectChanges();
    }
  }

  onGenerateToken(): void {
    if (!this.siteName.valid) {
      this.siteName.markAllAsTouched();
      return;
    }

    if (!this.filesystemName || !this.entityName) {
      return;
    }

    const fsName = this.filesystemName;
    const clientName = this.entityName;
    const siteName = this.siteName.value;

    this.isGenerating = true;

    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(`mirroring/token/create`, {
          fsName: fsName,
          clientName: clientName,
          siteName: siteName
        }),
        call: this.cephfsService.createBootstrapToken(fsName, clientName, siteName).pipe(
          tap((res: TokenResponse) => {
            if (res && (res?.token || res?.data)) {
              this.generatedToken = res.token || res.data;
              this.cdr.detectChanges();
            } else if (typeof res === 'string') {
              this.generatedToken = res;
              this.cdr.detectChanges();
            }
          }),
          map((res: TokenResponse) => {
            return { ...(res as Record<string, unknown>), __taskCompleted: true };
          })
        )
      })
      .subscribe({
        complete: () => {
          this.isGenerating = false;
        },
        error: () => {
          this.isGenerating = false;
        }
      });
  }

  downloadToken(): void {
    if (this.generatedToken) {
      const element = document.createElement('a');
      element.setAttribute(
        'href',
        'data:text/plain;charset=utf-8,' + encodeURIComponent(this.generatedToken)
      );
      element.setAttribute('download', `cephfs-bootstrap-token-${this.siteName.value}.txt`);
      element.style.display = 'none';
      document.body.appendChild(element);
      element.click();
      document.body.removeChild(element);
    }
  }

  onDismissSuccessAlert(): void {
    this.showSuccessAlert = false;
  }

  onDismissWarningAlert(): void {
    this.showWarningAlert = false;
  }

  onDismissInfoAlert(): void {
    this.showInfoAlert = false;
  }

  onDismissEnvironmentAlert(): void {
    this.showEnvironmentAlert = false;
  }
}
