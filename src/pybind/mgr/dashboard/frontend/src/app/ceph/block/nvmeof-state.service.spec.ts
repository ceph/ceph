import { TestBed } from '@angular/core/testing';

import { Subject } from 'rxjs';

import { NvmeofStateService } from './nvmeof-state.service';
import { SummaryService } from '~/app/shared/services/summary.service';

describe('NvmeofStateService', () => {
  let service: NvmeofStateService;
  let summaryData$: Subject<any>;
  let summaryServiceSpy: any;

  const emitSummary = (tasks: any[]) => summaryData$.next({ finished_tasks: tasks });

  beforeEach(() => {
    summaryData$ = new Subject<any>();
    summaryServiceSpy = {
      subscribe: jest.fn().mockImplementation((callback: (s: any) => void) => {
        return summaryData$.subscribe(callback);
      })
    };

    TestBed.configureTestingModule({
      providers: [NvmeofStateService, { provide: SummaryService, useValue: summaryServiceSpy }]
    });

    service = TestBed.inject(NvmeofStateService);
  });

  it('should create', () => {
    expect(service).toBeTruthy();
  });

  it('should emit refresh$ when requestRefresh is called', (done) => {
    service.refresh$.subscribe(() => done());
    service.requestRefresh();
  });

  it('should not emit refresh$ on the first summary update (initialization baseline)', () => {
    const refreshSpy = jest.fn();
    service.refresh$.subscribe(refreshSpy);

    emitSummary([
      {
        name: 'service/delete',
        begin_time: '2026-01-01T00:00:00Z',
        metadata: { service_name: 'nvmeof.rbd.default' }
      }
    ]);

    expect(refreshSpy).not.toHaveBeenCalled();
  });

  it('should emit refresh$ when a new NVMe service/delete task appears', () => {
    const refreshSpy = jest.fn();
    service.refresh$.subscribe(refreshSpy);

    emitSummary([]); // init
    emitSummary([
      {
        name: 'service/delete',
        begin_time: '2026-01-01T00:00:00Z',
        metadata: { service_name: 'nvmeof.rbd.default' }
      }
    ]);

    expect(refreshSpy).toHaveBeenCalledTimes(1);
  });

  it('should emit refresh$ when a new NVMe service/create task appears', () => {
    const refreshSpy = jest.fn();
    service.refresh$.subscribe(refreshSpy);

    emitSummary([]); // init
    emitSummary([
      {
        name: 'service/create',
        begin_time: '2026-01-01T00:00:00Z',
        metadata: { service_name: 'nvmeof.rbd.default' }
      }
    ]);

    expect(refreshSpy).toHaveBeenCalledTimes(1);
  });

  it('should emit refresh$ when a nvmeof/subsystem/delete task appears', () => {
    const refreshSpy = jest.fn();
    service.refresh$.subscribe(refreshSpy);

    emitSummary([]); // init
    emitSummary([
      {
        name: 'nvmeof/subsystem/delete',
        begin_time: '2026-01-01T00:00:00Z',
        metadata: { nqn: 'sub1' }
      }
    ]);

    expect(refreshSpy).toHaveBeenCalledTimes(1);
  });

  it('should emit refresh$ when a nvmeof/namespace/create task appears', () => {
    const refreshSpy = jest.fn();
    service.refresh$.subscribe(refreshSpy);

    emitSummary([]); // init
    emitSummary([
      {
        name: 'nvmeof/namespace/create',
        begin_time: '2026-01-01T00:00:00Z',
        metadata: { nqn: 'sub1' }
      }
    ]);

    expect(refreshSpy).toHaveBeenCalledTimes(1);
  });

  it('should emit refresh$ when a nvmeof/namespace/delete task appears', () => {
    const refreshSpy = jest.fn();
    service.refresh$.subscribe(refreshSpy);

    emitSummary([]); // init
    emitSummary([
      {
        name: 'nvmeof/namespace/delete',
        begin_time: '2026-01-01T00:00:00Z',
        metadata: { nsid: 1, nqn: 'sub1' }
      }
    ]);

    expect(refreshSpy).toHaveBeenCalledTimes(1);
  });

  it('should not emit refresh$ for non-NVMe tasks', () => {
    const refreshSpy = jest.fn();
    service.refresh$.subscribe(refreshSpy);

    emitSummary([]); // init
    emitSummary([{ name: 'rbd/create', begin_time: '2026-01-01T00:00:00Z', metadata: {} }]);

    expect(refreshSpy).not.toHaveBeenCalled();
  });

  it('should not emit refresh$ for service/delete of a non-NVMe service', () => {
    const refreshSpy = jest.fn();
    service.refresh$.subscribe(refreshSpy);

    emitSummary([]); // init
    emitSummary([
      {
        name: 'service/delete',
        begin_time: '2026-01-01T00:00:00Z',
        metadata: { service_name: 'rbd.default' }
      }
    ]);

    expect(refreshSpy).not.toHaveBeenCalled();
  });

  it('should not emit refresh$ for the same task appearing again', () => {
    const refreshSpy = jest.fn();
    service.refresh$.subscribe(refreshSpy);

    const task = {
      name: 'service/delete',
      begin_time: '2026-01-01T00:00:00Z',
      metadata: { service_name: 'nvmeof.rbd.default' }
    };

    emitSummary([]); // init
    emitSummary([task]); // new task — emits once
    emitSummary([task]); // same task — no second emit

    expect(refreshSpy).toHaveBeenCalledTimes(1);
  });

  it('should unsubscribe from SummaryService on destroy', () => {
    const unsubscribeSpy = jest.fn();
    const mockSubscription = { unsubscribe: unsubscribeSpy };
    summaryServiceSpy.subscribe.mockReturnValueOnce(mockSubscription);

    const freshService = new NvmeofStateService(summaryServiceSpy as any);
    freshService.ngOnDestroy();

    expect(unsubscribeSpy).toHaveBeenCalled();
  });
});
