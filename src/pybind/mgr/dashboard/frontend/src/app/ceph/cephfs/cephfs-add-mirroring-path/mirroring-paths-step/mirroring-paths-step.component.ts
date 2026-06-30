import { Component, DestroyRef, inject, Input, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormControl } from '@angular/forms';
import { forkJoin, Observable, of } from 'rxjs';
import { catchError, finalize, map, switchMap, take } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { MirroringPathUtils } from '../mirroring-path-utils';
import { createPathEntry, PathEntry } from '../mirroring-path.model';

const VOLUMES_ROOT = '/volumes';
const LS_DEPTH = 1;

@Component({
  selector: 'cd-mirroring-paths-step',
  templateUrl: './mirroring-paths-step.component.html',
  styleUrls: ['./mirroring-paths-step.component.scss'],
  standalone: false
})
export class MirroringPathsStepComponent implements OnInit, TearsheetStep {
  @Input() fsName: string;
  @Input() fsId: number;

  formGroup!: CdFormGroup;
  paths: PathEntry[] = [];
  loadingLevels: Record<string, true> = {};

  private trackedPaths = new Set<string>();
  private destroyRef = inject(DestroyRef);
  private cephfsService = inject(CephfsService);

  ngOnInit(): void {
    this.formGroup = new CdFormGroup({
      pathsControl: new FormControl<string[]>([], { nonNullable: true })
    });
    this.paths = [createPathEntry()];
    this.syncFormValue();
    this.loadInitialData();
  }

  get pathsControl(): FormControl<string[]> {
    return this.formGroup.get('pathsControl') as FormControl<string[]>;
  }

  get pathsError(): string {
    const control = this.pathsControl;
    if (!control.invalid || !(control.touched || control.dirty)) {
      return '';
    }
    if (control.hasError('alreadyMirrored')) {
      return $localize`Selected path(s) are already mirrored. Select a path that is not already mirrored.`;
    }
    return $localize`Select at least one path to continue.`;
  }

  addPath(): void {
    this.paths.push(createPathEntry());
    this.loadLevelOptions(this.paths.length - 1, 0, VOLUMES_ROOT);
  }

  removePath(index: number): void {
    this.paths.splice(index, 1);
    this.syncFormValue();
  }

  toggleExpand(index: number): void {
    this.paths[index].expanded = !this.paths[index].expanded;
  }

  isLevelLoading(pathIndex: number, levelIndex: number): boolean {
    return !!this.loadingLevels[`${pathIndex}:${levelIndex}`];
  }

  onLevelChange(pathIndex: number, levelIndex: number, selected: string): void {
    const entry = this.paths[pathIndex];
    if (!entry) {
      return;
    }

    const levels = entry.levels.map((level, i) =>
      i === levelIndex ? { ...level, selected } : level
    );
    levels.splice(levelIndex + 1);

    const updated: PathEntry = {
      ...entry,
      levels,
      fullPath: MirroringPathUtils.buildPathFromSegments(
        levels.map((level) => level.selected).filter(Boolean)
      )
    };
    this.paths[pathIndex] = updated;

    if (!selected) {
      this.syncFormValue();
      return;
    }

    this.loadLevelOptions(pathIndex, levelIndex + 1, updated.fullPath);
  }

  getSubmitPaths(): { toAdd: string[]; alreadyMirrored: string[] } {
    const toAdd: string[] = [];
    const alreadyMirrored: string[] = [];

    this.paths.forEach((entry, pathIndex) => {
      if (!entry.fullPath) {
        return;
      }
      const path = MirroringPathUtils.normalizePath(entry.fullPath);
      if (!path) {
        return;
      }
      if (MirroringPathUtils.isPathTracked(path, this.trackedPaths)) {
        alreadyMirrored.push(path);
      } else if (this.isPathSelectable(path, pathIndex)) {
        toAdd.push(path);
      }
    });

    return { toAdd, alreadyMirrored };
  }

  addTrackedPath(path: string): void {
    const normalized = MirroringPathUtils.normalizePath(path);
    if (normalized) {
      this.trackedPaths.add(normalized);
      this.syncFormValue();
    }
  }

  refreshTrackedPaths(): Observable<void> {
    if (!this.fsName) {
      return of(undefined);
    }
    return this.cephfsService.listMirrorDirectories(this.fsName).pipe(
      map((paths) => {
        this.trackedPaths = new Set(paths.map(MirroringPathUtils.normalizePath).filter(Boolean));
        this.syncFormValue();
      }),
      catchError(() => of(undefined)),
      take(1)
    );
  }

  private loadInitialData(): void {
    if (!this.fsName) {
      return;
    }

    this.resolveFsId()
      .pipe(
        switchMap((fsId) =>
          forkJoin([
            of(fsId),
            this.cephfsService.listMirrorDirectories(this.fsName).pipe(catchError(() => of([])))
          ])
        ),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe(([fsId, trackedList]) => {
        this.trackedPaths = new Set(
          trackedList.map(MirroringPathUtils.normalizePath).filter(Boolean)
        );
        if (fsId) {
          this.loadLevelOptions(0, 0, VOLUMES_ROOT);
        }
      });
  }

  private resolveFsId(): Observable<number> {
    if (this.fsId) {
      return of(this.fsId);
    }
    return this.cephfsService.list().pipe(
      map((filesystems: { id?: number; mdsmap?: { fs_name?: string } }[]) => {
        const id = filesystems.find((fs) => fs.mdsmap?.fs_name === this.fsName)?.id ?? 0;
        this.fsId = id;
        return id;
      }),
      catchError(() => of(0))
    );
  }

  private loadLevelOptions(pathIndex: number, levelIndex: number, parentPath: string): void {
    if (!this.fsId) {
      return;
    }

    const loadingKey = `${pathIndex}:${levelIndex}`;
    this.loadingLevels = { ...this.loadingLevels, [loadingKey]: true };

    this.cephfsService
      .lsDir(this.fsId, parentPath, LS_DEPTH)
      .pipe(
        take(1),
        catchError(() => of([])),
        finalize(() => {
          const { [loadingKey]: _, ...rest } = this.loadingLevels;
          this.loadingLevels = rest;
        }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe((dirs) => {
        const entry = this.paths[pathIndex];
        if (!entry) {
          return;
        }

        const options = dirs
          .map((dir) => dir.name)
          .filter((name) =>
            this.isPathSelectable(MirroringPathUtils.joinPath(parentPath, name), pathIndex)
          )
          .sort();

        const levels = [...entry.levels];
        if (levelIndex < levels.length) {
          levels[levelIndex] = { ...levels[levelIndex], options };
        } else if (options.length) {
          levels.push({ options, selected: '' });
        }

        this.paths[pathIndex] = { ...entry, levels };
        this.syncFormValue();
      });
  }

  private isPathSelectable(path: string, pathIndex: number): boolean {
    const normalized = MirroringPathUtils.normalizePath(path);
    if (!normalized || MirroringPathUtils.isPathTracked(normalized, this.trackedPaths)) {
      return false;
    }

    return !this.paths.some((entry, index) => {
      if (index === pathIndex) {
        return false;
      }
      const selected = MirroringPathUtils.normalizePath(entry.fullPath);
      return selected && MirroringPathUtils.pathsOverlap(normalized, selected);
    });
  }

  private syncFormValue(): void {
    const { toAdd, alreadyMirrored } = this.getSubmitPaths();
    const control = this.pathsControl;
    control.setValue(toAdd, { emitEvent: false });

    if (toAdd.length) {
      control.setErrors(null);
    } else if (alreadyMirrored.length) {
      control.setErrors({ alreadyMirrored: true });
    } else {
      control.setErrors({ required: true });
    }
  }
}
