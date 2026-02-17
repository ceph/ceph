import { Component, Input, OnInit, OnDestroy, OnChanges, SimpleChanges, ChangeDetectorRef, inject } from "@angular/core";
import { FormControl, Validators } from "@angular/forms";
import { map, tap } from "rxjs/operators";
import { CephfsService } from "~/app/shared/api/cephfs.service";
import { TaskWrapperService } from "~/app/shared/services/task-wrapper.service";
import { FinishedTask } from "~/app/shared/models/finished-task";
import { HealthService } from "~/app/shared/api/health.service";

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
export class CephfsGenerateTokenComponent implements OnInit, OnDestroy, OnChanges {
  @Input() selectedFilesystem: any;
  @Input() selectedEntity: any;

  filesystemName: string = '';
  entityName: string = '';
  siteName: FormControl;
  siteNameError: string = $localize`Site name is required`;
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
    this.siteName = new FormControl('', Validators.required);

    // Fetch cluster ID
    this.healthService.getHealthSnapshot().subscribe((snapshot: any) => {
      this.clusterId = snapshot?.fsid || '';
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['selectedFilesystem'] && this.selectedFilesystem) {
      this.filesystemName = this.selectedFilesystem?.name || '';
      this.cdr.detectChanges();
    }
    if (changes['selectedEntity'] && this.selectedEntity) {
      if (typeof this.selectedEntity === 'string') {
        this.entityName = this.selectedEntity;
      } else {
        this.entityName = this.selectedEntity?.entity || this.selectedEntity?.user_entity || '';
      }
      this.cdr.detectChanges();
    }
  }

  ngOnDestroy(): void {
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
        call: this.cephfsService.createBootstrapToken(
          fsName,
          clientName,
          siteName
        ).pipe(
          tap((res: TokenResponse) => {
            console.log('Raw API response:', res);
            if (res && (res?.token || res?.data)) {
              this.generatedToken = res.token || res.data;
              console.log('Token set to:', this.generatedToken);
              this.cdr.detectChanges();
            } else if (typeof res === 'string') {
              this.generatedToken = res;
              this.cdr.detectChanges();
            }
          }),
          map((res: TokenResponse) => {
            return { ...res, __taskCompleted: true };
          })
        )
      })
      .subscribe({
        next: (res: any) => {
          console.log('TaskWrapper next response:', res);
        },
        complete: () => {
          console.log('Bootstrap token generated successfully');
          this.isGenerating = false;
        },
        error: (err) => {
          console.error('Error generating bootstrap token', err);
          this.isGenerating = false;
        }
      });
  }

  toggleTokenExpanded(): void {
    // No longer needed with textarea
  }

  copyToken(): void {
    if (this.generatedToken) {
      navigator.clipboard.writeText(this.generatedToken).then(() => {
        console.log('Token copied to clipboard');
      }).catch((err) => {
        console.error('Failed to copy token:', err);
      });
    }
  }

  downloadToken(): void {
    if (this.generatedToken) {
      const element = document.createElement('a');
      element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(this.generatedToken));
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