import { Injectable } from '@angular/core';
import { ReplaySubject } from 'rxjs';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsDetail } from '~/app/shared/models/cephfs.model';

@Injectable()
export class CephfsResourceStateService {
  private filesystemSource = new ReplaySubject<CephfsDetail | null>(1);

  readonly filesystem$ = this.filesystemSource.asObservable();

  constructor(private cephfsService: CephfsService) {}

  load(fsId: string): void {
    const id = Number(fsId);

    if (!Number.isFinite(id) || id <= 0) {
      this.filesystemSource.next(null);
      return;
    }

    (this.cephfsService.list() as any).subscribe({
      next: (filesystems: CephfsDetail[]) => {
        const filesystem = (filesystems || []).find((fs) => fs.id === id) ?? null;
        this.filesystemSource.next(filesystem);
      },
      error: () => this.filesystemSource.next(null)
    });
  }
}
