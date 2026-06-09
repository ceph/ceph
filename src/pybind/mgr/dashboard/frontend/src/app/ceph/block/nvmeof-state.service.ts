import { Injectable, OnDestroy } from '@angular/core';

import { merge, Subject, Subscription } from 'rxjs';

import { FinishedTask } from '~/app/shared/models/finished-task';
import { Summary } from '~/app/shared/models/summary.model';
import { SummaryService } from '~/app/shared/services/summary.service';

@Injectable({
  providedIn: 'root'
})
export class NvmeofStateService implements OnDestroy {
  private readonly explicitRefreshSource = new Subject<void>();
  private readonly taskRefreshSource = new Subject<void>();
  private summarySubscription: Subscription;
  private seenTaskSignatures = new Set<string>();
  private summaryInitialized = false;

  readonly refresh$ = merge(
    this.explicitRefreshSource.asObservable(),
    this.taskRefreshSource.asObservable()
  );

  constructor(private summaryService: SummaryService) {
    this.summarySubscription = this.summaryService.subscribe((summary: Summary) => {
      const nvmeTasks = (summary?.finished_tasks ?? []).filter((t: FinishedTask) =>
        this.isNvmeTask(t)
      );
      const currentSignatures = new Set(
        nvmeTasks.map((t: FinishedTask) => this.getTaskSignature(t))
      );

      if (!this.summaryInitialized) {
        this.summaryInitialized = true;
        this.seenTaskSignatures = currentSignatures;
        return;
      }

      const hasNewTask = [...currentSignatures].some((sig) => !this.seenTaskSignatures.has(sig));
      this.seenTaskSignatures = currentSignatures;

      if (hasNewTask) {
        this.taskRefreshSource.next();
      }
    });
  }

  ngOnDestroy(): void {
    this.summarySubscription?.unsubscribe();
  }

  requestRefresh(): void {
    this.explicitRefreshSource.next();
  }

  private isNvmeTask(task: FinishedTask): boolean {
    if (task?.name === 'service/create' || task?.name === 'service/delete') {
      return (
        typeof task?.metadata?.['service_name'] === 'string' &&
        (task.metadata['service_name'] as string).startsWith('nvmeof.')
      );
    }
    return [
      'nvmeof/subsystem/delete',
      'nvmeof/namespace/create',
      'nvmeof/namespace/delete'
    ].includes(task?.name);
  }

  private getTaskSignature(task: FinishedTask): string {
    return JSON.stringify({
      name: task.name,
      end_time: task.end_time,
      metadata: task.metadata
    });
  }
}
