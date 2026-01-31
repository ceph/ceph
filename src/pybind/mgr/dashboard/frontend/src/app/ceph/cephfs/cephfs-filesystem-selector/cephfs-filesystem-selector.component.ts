import { Component, OnInit, TemplateRef, ViewChild, inject, Input } from '@angular/core';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Icons } from '~/app/shared/enum/icons.enum';
import { catchError } from 'rxjs/operators';
import { forkJoin, of, Observable, BehaviorSubject } from 'rxjs';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { CephfsDetail, FilesystemRow } from '~/app/shared/models/cephfs.model';

@Component({
  selector: 'cd-cephfs-filesystem-selector',
  templateUrl: './cephfs-filesystem-selector.component.html',
  standalone: false,
  styleUrls: ['./cephfs-filesystem-selector.component.scss']
})
export class CephfsFilesystemSelectorComponent implements OnInit {
  @ViewChild('mdsStatus', { static: true })
  mdsStatus: TemplateRef<any>;
  @ViewChild('mirroringStatus', { static: true })
  mirroringStatus: TemplateRef<any>;
  private cephfsService = inject(CephfsService);
  private dimlessBinaryPipe = inject(DimlessBinaryPipe);
  columns: CdTableColumn[] = [];
  filesystems: FilesystemRow[] = [];
  selection = new CdTableSelection();
  icons = Icons;
 @Input() selectedFilesystem$: BehaviorSubject<any>;

  ngOnInit(): void {
    this.columns = [
      { name: $localize`Filesystem name`, prop: 'name', flexGrow: 2 },
      { name: $localize`Usage`, prop: 'used', flexGrow: 1, pipe: this.dimlessBinaryPipe },
      {
        prop: $localize`pools`,
        name: 'Pools used',
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          class: 'tag-background-primary'
        },
        flexGrow: 1.3
      },
      { name: $localize`Status`, prop: 'mdsStatus', flexGrow: 0.8, cellTemplate: this.mdsStatus },

      {
        name: $localize`Mirroring status`,
        prop: 'mirroringStatus',
        flexGrow: 0.8,
        cellTemplate: this.mirroringStatus
      }
    ];

    this.loadFilesystems();
  }

  loadFilesystems() {
    this.cephfsService.list().subscribe((fsList: Array<{ id: number }>) => {
      if (!fsList || fsList.length === 0) {
        this.filesystems = [];
        return;
      }

      const detailRequests = fsList.map(
        (fs): Observable<CephfsDetail | null> =>
          this.cephfsService.getCephfs(fs.id).pipe(catchError(() => of(null)))
      );

      forkJoin(detailRequests).subscribe((details: Array<CephfsDetail | null>) => {
        this.filesystems = details
          .filter((d) => d !== null)
          .map((detail) => {
            const pools = detail.cephfs?.pools || [];
            const poolNames = pools.map((p) => p.pool);
            const totalUsed = pools.reduce((sum, p) => sum + p.used, 0);

            return {
              id: detail.cephfs.id,
              name: detail.cephfs.name,
              pools: poolNames,
              used: `${totalUsed}`,
              mdsStatus: detail.cephfs.flags?.enabled ? $localize`Active` : $localize`Inactive`,
              mirroringStatus:
                Object.keys(detail.cephfs?.mirror_info?.peers || {}).length > 0
                  ? $localize`Enabled`
                  : $localize`Disabled`
            };
          });
      });
    });
  }

 updateSelection(selection: CdTableSelection) {
    this.selection = selection;
    this.selectedFilesystem$?.next(selection?.selected?.[0] ?? null);
  }
}
