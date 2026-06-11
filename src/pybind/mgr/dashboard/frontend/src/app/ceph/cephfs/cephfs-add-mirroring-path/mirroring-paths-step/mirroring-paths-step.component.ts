import { Component, Input, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { of } from 'rxjs';
import { catchError } from 'rxjs/operators';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { DEFAULT_SUBVOLUME_GROUP } from '~/app/shared/constants/cephfs.constant';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

export interface DirLevel {
  options: string[];
  selected: string;
  kind: 'group' | 'subvolume' | 'dir';
}

export interface PathEntry {
  fullPath: string;
  levels: DirLevel[];
  expanded: boolean;
  subvolumePath?: string;
  filePickerPath?: string;
}

@Component({
  selector: 'cd-mirroring-paths-step',
  templateUrl: './mirroring-paths-step.component.html',
  styleUrls: ['./mirroring-paths-step.component.scss'],
  standalone: false
})
export class MirroringPathsStepComponent implements OnInit, TearsheetStep {
  @Input() fsName: string;
  @Input() fsId: number;

  formGroup: CdFormGroup;
  paths: PathEntry[] = [];

  constructor(
    private cephfsService: CephfsService,
    private subvolumeGroupService: CephfsSubvolumeGroupService,
    private subvolumeService: CephfsSubvolumeService
  ) {}

  ngOnInit() {
    this.formGroup = new CdFormGroup({
      pathsControl: new UntypedFormControl([], Validators.required)
    });
    this.resolveFsId();
    this.loadSubvolumeGroups();
  }

  private resolveFsId() {
    if (this.fsId) return;
    this.cephfsService.list().subscribe((filesystems: any[]) => {
      const match = filesystems.find((fs) => fs.mdsmap?.fs_name === this.fsName);
      if (match) {
        this.fsId = match.id;
      }
    });
  }

  private loadSubvolumeGroups() {
    this.subvolumeGroupService
      .get(this.fsName, false)
      .pipe(catchError(() => of([])))
      .subscribe((groups: any[]) => {
        const groupNames = groups.map((g) => g.name).filter(Boolean);
        if (this.paths.length === 0) {
          this.paths.push({
            fullPath: '',
            levels: [{ options: groupNames, selected: '', kind: 'group' }],
            expanded: true
          });
        } else {
          this.paths.forEach((p) => {
            if (p.levels[0]) p.levels[0].options = groupNames;
          });
        }
      });
  }

  addPath() {
    const groupOptions = this.paths[0]?.levels[0]?.options ?? [];
    this.paths.push({
      fullPath: '',
      levels: [{ options: [...groupOptions], selected: '', kind: 'group' }],
      expanded: true
    });
  }

  removePath(index: number) {
    this.paths.splice(index, 1);
    this.syncFormValue();
  }

  onLevelChange(pathIndex: number, levelIndex: number, selected: string) {
    const entry = this.paths[pathIndex];
    if (!entry) return;

    entry.levels[levelIndex].selected = selected;
    entry.levels.splice(levelIndex + 1);

    if (!selected) {
      entry.fullPath = '';
      entry.subvolumePath = undefined;
      this.syncFormValue();
      return;
    }

    const currentKind = entry.levels[levelIndex].kind;

    if (currentKind === 'group') {
      entry.fullPath = '';
      entry.subvolumePath = undefined;
      this.subvolumeService
        .get(this.fsName, selected === DEFAULT_SUBVOLUME_GROUP ? '' : selected, false)
        .pipe(catchError(() => of([])))
        .subscribe((subvols: any[]) => {
          const subvolNames = subvols.map((s) => s.name).filter(Boolean);
          if (this.paths[pathIndex]) {
            this.paths[pathIndex].levels = [
              ...this.paths[pathIndex].levels,
              { options: subvolNames, selected: '', kind: 'subvolume' as const }
            ];
          }
        });
      this.syncFormValue();
      return;
    }

    if (currentKind === 'subvolume') {
      const group = entry.levels[0].selected;
      this.subvolumeService
        .info(this.fsName, selected, group === DEFAULT_SUBVOLUME_GROUP ? '' : group)
        .pipe(catchError(() => of({ path: null })))
        .subscribe((info: any) => {
          if (!this.paths[pathIndex]) return;
          const resolvedPath = info?.path ?? `/${selected}`;
          const parentPath =
            resolvedPath.split('/').slice(0, -1).join('/') || '/';
          const lastSegment = resolvedPath.split('/').filter(Boolean).pop();
          this.paths[pathIndex].subvolumePath = parentPath;
          this.cephfsService
            .lsDir(this.fsId, parentPath, 1)
            .pipe(catchError(() => of([])))
            .subscribe((dirs: any[]) => {
              if (!this.paths[pathIndex]) return;
              const childNames = dirs
                .filter((d) => d.path !== parentPath)
                .map((d) => d.name)
                .filter(Boolean);
              if (childNames.length > 0) {
                this.paths[pathIndex].levels = [
                  ...this.paths[pathIndex].levels,
                  { options: childNames, selected: '', kind: 'dir' as const }
                ];
              } else if (lastSegment) {
                this.paths[pathIndex].levels = [
                  ...this.paths[pathIndex].levels,
                  { options: [lastSegment], selected: '', kind: 'dir' as const }
                ];
              } else {
                this.paths[pathIndex].fullPath = resolvedPath;
                this.syncFormValue();
              }
            });
        });
      return;
    }

    const dirSegments = entry.levels
      .slice(entry.levels.findIndex((l) => l.kind === 'dir'))
      .map((l) => l.selected)
      .filter(Boolean);
    entry.fullPath = entry.subvolumePath + (dirSegments.length ? '/' + dirSegments.join('/') : '');
    this.syncFormValue();

    this.cephfsService
      .lsDir(this.fsId, entry.fullPath, 1)
      .pipe(catchError(() => of([])))
      .subscribe((dirs: any[]) => {
        const childNames = dirs
          .filter((d) => d.path !== entry.fullPath)
          .map((d) => d.name)
          .filter(Boolean);
        if (childNames.length > 0 && this.paths[pathIndex]) {
          this.paths[pathIndex].levels = [
            ...this.paths[pathIndex].levels,
            { options: childNames, selected: '', kind: 'dir' as const }
          ];
        }
      });
  }

  toggleExpand(index: number) {
    this.paths[index].expanded = !this.paths[index].expanded;
  }

  onFileSelected(index: number, event: Event) {
    const input = event.target as HTMLInputElement;
    const file = input?.files?.[0];
    if (!file) return;
    const path = (file as any).webkitRelativePath || file.name;
    const folderPath = '/' + path.split('/')[0];
    const entry = this.paths[index];
    entry.fullPath = folderPath;
    entry.filePickerPath = folderPath;
    this.syncFormValue();
    input.value = '';
  }

  onFilePickerSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    const file = input?.files?.[0];
    if (!file) return;
    const path = (file as any).webkitRelativePath || file.name;
    const folderPath = '/' + path.split('/')[0];
    this.paths.push({
      fullPath: folderPath,
      levels: [],
      expanded: false,
      filePickerPath: folderPath
    });
    this.syncFormValue();
    input.value = '';
  }

  private syncFormValue() {
    const validPaths = this.paths.filter((p) => !!p.fullPath).map((p) => p.fullPath);
    this.formGroup?.get('pathsControl')?.setValue(validPaths);
    this.formGroup?.get('pathsControl')?.updateValueAndValidity();
  }

  getValidPaths(): string[] {
    return this.paths.filter((p) => !!p.fullPath).map((p) => p.fullPath);
  }
}
