import { Component, OnInit, TemplateRef, ViewChild, inject } from '@angular/core';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Icons } from '~/app/shared/enum/icons.enum';
import { catchError, map, switchMap } from 'rxjs/operators';
import { forkJoin, of, Observable } from 'rxjs';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import {
  CephfsDetail,
  FilesystemRow,
  MdsStatus,
  MirroringStatus,
  MIRRORING_STATUS,
  mdsStateToStatus
} from '~/app/shared/models/cephfs.model';

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
  columns: CdTableColumn[] = [];
  filesystems$: Observable<FilesystemRow[]> = of([]);
  selection = new CdTableSelection();
  icons = Icons;
  mdsStatusLabels: Record<MdsStatus, string> = {
    Active: $localize`Active`,
    Warning: $localize`Warning`,
    Inactive: $localize`Inactive`
  };
  mirroringStatusLabels: Record<MirroringStatus, string> = {
    Enabled: $localize`Enabled`,
    Disabled: $localize`Disabled`
  };

  private cephfsService = inject(CephfsService);
  private dimlessBinaryPipe = inject(DimlessBinaryPipe);

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

    this.filesystems$ = this.cephfsService.list().pipe(
      switchMap((listResponse: Array<CephfsDetail>) => {
        if (!listResponse?.length) {
          return of([]);
        }
        const detailRequests = listResponse.map(
          (fs): Observable<CephfsDetail | null> =>
            this.cephfsService.getCephfs(fs.id).pipe(catchError(() => of(null)))
        );
        return forkJoin(detailRequests).pipe(
          map((details: Array<CephfsDetail | null>) =>
            details
              .map((detail, index) => {
                if (!detail?.cephfs) {
                  return null;
                }
                const listItem = listResponse[index];
                const pools = detail.cephfs.pools || [];
                const poolNames = pools.map((p) => p.pool);
                const totalUsed = pools.reduce((sum, p) => sum + p.used, 0);
                const mdsInfo = listItem?.mdsmap?.info ?? {};
                const firstMdsGid = Object.keys(mdsInfo)[0];
                const mdsState = firstMdsGid ? mdsInfo[firstMdsGid]?.state : undefined;
                return {
                  id: detail.cephfs.id,
                  name: detail.cephfs.name,
                  pools: poolNames,
                  used: `${totalUsed}`,
                  mdsStatus: mdsStateToStatus(mdsState),
                  mirroringStatus: listItem?.mirror_info
                    ? MIRRORING_STATUS.Enabled
                    : MIRRORING_STATUS.Disabled
                } as FilesystemRow;
              })
              .filter((row): row is FilesystemRow => row !== null)
          )
        );
      })
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
