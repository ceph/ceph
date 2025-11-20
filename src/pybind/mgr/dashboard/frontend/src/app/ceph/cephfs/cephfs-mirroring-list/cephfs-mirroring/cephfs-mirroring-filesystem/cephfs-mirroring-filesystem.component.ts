import { Component, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Icons } from '~/app/shared/enum/icons.enum';
import { catchError } from 'rxjs/operators';
import { BehaviorSubject, forkJoin, of } from 'rxjs';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';

@Component({
  selector: 'cd-cephfs-mirroring-filesystem',
  templateUrl: './cephfs-mirroring-filesystem.component.html',
  standalone: false,
  styleUrls: ['./cephfs-mirroring-filesystem.component.scss']
})
export class CephfsMirroringFilesystemComponent implements OnInit {
  @ViewChild('deleteTpl', { static: true }) deleteTpl: TemplateRef<any>;
  @ViewChild('mdsStatus', { static: true })
  mdsStatus: TemplateRef<any>;
  @ViewChild('mirroringStatus', { static: true })
  mirroringStatus: TemplateRef<any>;
  columns: CdTableColumn[] = [];
  filesystems: any[] = [];
  selection = new CdTableSelection();
  icons = Icons;
  @Input() selectedFilesystem$: BehaviorSubject<any>;

  selectionInfo = {
    kind: 'info',
    title: 'Selection requirements',
    lowContrast: true,
    showClose: true
  };

  notificationType = 'info';
  lowContrast = true;
  showClose = true;

  constructor(private cephfsService: CephfsService) {}

  ngOnInit(): void {
    this.columns = [
      { name: 'Filesystem Name', prop: 'name', flexGrow: 2 },
      { name: 'Used (and %)', prop: 'used', flexGrow: 1 },
      {
        prop: 'pools',
        name: $localize`Pools Used`,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          class: 'tag-background-primary'
        },
        flexGrow: 1.3
      },
      { name: 'MDS Status', prop: 'mdsStatus', flexGrow: 0.8, cellTemplate: this.mdsStatus },

      {
        name: 'Mirroring Status',
        prop: 'mirroringStatus',
        flexGrow: 0.8,
        cellTemplate: this.mirroringStatus
      }
    ];

    this.loadFilesystems();
  }

  loadFilesystems() {
    this.cephfsService.list().subscribe((fsList: any[]) => {
      if (!fsList || fsList.length === 0) {
        this.filesystems = [];
        return;
      }

      const detailRequests = fsList.map((fs) =>
        this.cephfsService.getCephfs(fs.id).pipe(catchError(() => of(null)))
      );

      forkJoin(detailRequests).subscribe((details: any[]) => {
        this.filesystems = details
          .filter((d) => d !== null)
          .map((detail) => {
            const pools = detail.cephfs?.pools || [];
            const poolNames = pools.map((p: { pool: any }) => p.pool);
            const totalUsed = pools.reduce((sum: any, p: { used: any }) => sum + p.used, 0);
            const totalAvail = pools.reduce((sum: any, p: { avail: any }) => sum + p.avail, 0);
            const totalSize = totalUsed + totalAvail;
            const usedPercent = totalSize ? (totalUsed / totalSize) * 100 : 0;

            return {
              id: detail.cephfs.id,
              name: detail.cephfs.name,
              pools: poolNames,
              used: `${this.formatSize(totalUsed)} (${usedPercent.toFixed(0)}%)`,
              mdsStatus: detail.cephfs.flags?.enabled ? 'Active' : 'Inactive',
              mirroringStatus:
                Object.keys(detail.cephfs?.mirror_info?.peers || {}).length > 0
                  ? 'Enabled'
                  : 'Disabled'
            };
          });
      });
    });
  }

  // updateSelection(selection: CdTableSelection) {
  //   this.selection = selection;
  // }

  formatSize(size: number): string {
    if (size >= 1e9) return (size / 1e9).toFixed(2) + ' GB';
    if (size >= 1e6) return (size / 1e6).toFixed(2) + ' MB';
    if (size >= 1e3) return (size / 1e3).toFixed(2) + ' KB';
    return size + ' B';
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
    this.selectedFilesystem$?.next(selection?.selected?.[0] ?? null);
  }
}
