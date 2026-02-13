import { Component, Input } from '@angular/core';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { FilesystemRow } from '~/app/shared/models/cephfs.model';

@Component({
  selector: 'cd-cephfs-import-token',
  templateUrl: './cephfs-import-token.component.html',
  styleUrls: ['./cephfs-import-token.component.scss'],
  standalone: false
})
export class CephfsImportTokenComponent {
  @Input() selectedFilesystem: FilesystemRow | null = null;

  token: string = '';
  isSubmitting: boolean = false;

  constructor(private cephfsService: CephfsService) {}

  importToken(): void {
    let token = this.token?.trim();
    const filesystemName = this.selectedFilesystem?.name;
    if (!token || !filesystemName || this.isSubmitting) {
      return;
    }

    this.isSubmitting = true;
    this.cephfsService.createBootstrapPeer(filesystemName, token).subscribe({
      next: (response: any) => {
        if (response && typeof response === 'object') {
          this.token = JSON.stringify(response, null, 2);
        }
        this.isSubmitting = false;
      },
      error: () => {
        this.isSubmitting = false;
      }
    });
  }
}
