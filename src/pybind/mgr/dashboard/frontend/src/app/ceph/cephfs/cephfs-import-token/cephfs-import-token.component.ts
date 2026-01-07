import { Component, Input, OnChanges, SimpleChanges } from '@angular/core';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { FilesystemRow } from '~/app/shared/models/cephfs.model';

@Component({
  selector: 'cd-cephfs-import-token',
  templateUrl: './cephfs-import-token.component.html',
  styleUrls: ['./cephfs-import-token.component.scss'],
  standalone: false
})
export class CephfsImportTokenComponent implements OnChanges {
  @Input() selectedFilesystem: FilesystemRow | null = null;

  token = '';
  selectedFilesystemName: string | null = null;
  isSubmitting = false;

  constructor(private cephfsService: CephfsService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['selectedFilesystem']) {
      this.selectedFilesystemName = this.selectedFilesystem?.name ?? null;
    }
  }

  importToken(): void {
    const token = this.token?.trim();
    if (!token || !this.selectedFilesystemName || this.isSubmitting) {
      return;
    }

    this.isSubmitting = true;
    this.cephfsService.createBootstrapPeer(this.selectedFilesystemName, token).subscribe({
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
