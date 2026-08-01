import { Injectable, OnDestroy } from '@angular/core';
import { forkJoin, Observable, of, ReplaySubject, Subscription } from 'rxjs';
import { finalize, shareReplay, tap } from 'rxjs/operators';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { decodeModuleName, MgrModuleInfo } from '~/app/shared/models/mgr-modules.interface';

export interface MgrModuleResourceState {
  moduleNameRoute: string;
  moduleName: string;
  moduleInfo: MgrModuleInfo;
  moduleConfig: Record<string, unknown>;
}

@Injectable()
export class MgrModuleResourceStateService implements OnDestroy {
  private stateSource = new ReplaySubject<MgrModuleResourceState | null>(1);
  private loadSub = Subscription.EMPTY;
  private modulesCache: MgrModuleInfo[] | null = null;
  private modulesRequest$: Observable<MgrModuleInfo[]>;

  readonly state$ = this.stateSource.asObservable();

  constructor(private mgrModuleService: MgrModuleService) {}

  ngOnDestroy(): void {
    this.loadSub.unsubscribe();
  }

  load(moduleNameRoute: string): void {
    const moduleName = decodeModuleName(moduleNameRoute);
    if (!moduleName) {
      this.stateSource.next(null);
      return;
    }

    this.loadSub.unsubscribe();
    this.loadSub = forkJoin({
      modules: this.getModules(),
      moduleConfig: this.mgrModuleService.getConfig(moduleName)
    }).subscribe({
      next: ({ modules, moduleConfig }) => {
        const selectedModule = modules.find((module: MgrModuleInfo) => module.name === moduleName);

        if (!selectedModule) {
          this.stateSource.next(null);
          return;
        }

        this.stateSource.next({
          moduleNameRoute,
          moduleName,
          moduleInfo: selectedModule,
          moduleConfig: moduleConfig as Record<string, unknown>
        });
      },
      error: () => this.stateSource.next(null)
    });
  }

  private getModules(): Observable<MgrModuleInfo[]> {
    if (this.modulesCache) {
      return of(this.modulesCache);
    }

    if (!this.modulesRequest$) {
      this.modulesRequest$ = this.mgrModuleService.list().pipe(
        tap((modules: MgrModuleInfo[]) => (this.modulesCache = modules)),
        shareReplay(1),
        finalize(() => {
          this.modulesRequest$ = undefined;
        })
      );
    }

    return this.modulesRequest$;
  }
}
