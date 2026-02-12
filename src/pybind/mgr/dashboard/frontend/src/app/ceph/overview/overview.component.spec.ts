import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of, Subject, throwError } from 'rxjs';

import { OverviewComponent } from './overview.component';
import { HealthService } from '~/app/shared/api/health.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { HealthSnapshotMap } from '~/app/shared/models/health.interface';

describe('OverviewComponent (Jest)', () => {
  let component: OverviewComponent;
  let fixture: ComponentFixture<OverviewComponent>;

  let mockHealthService: {
    getHealthSnapshot: jest.Mock;
  };

  let mockRefreshIntervalService: {
    intervalData$: Subject<void>;
  };

  beforeEach(async () => {
    mockHealthService = {
      getHealthSnapshot: jest.fn()
    };

    mockRefreshIntervalService = {
      intervalData$: new Subject<void>()
    };

    await TestBed.configureTestingModule({
      imports: [OverviewComponent],
      providers: [
        { provide: HealthService, useValue: mockHealthService },
        { provide: RefreshIntervalService, useValue: mockRefreshIntervalService }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  // --------------------------------------------------
  // CREATION
  // --------------------------------------------------

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  // --------------------------------------------------
  // refreshIntervalObs - success case
  // --------------------------------------------------

  it('should call healthService when interval emits', (done) => {
    const mockResponse: HealthSnapshotMap = { status: 'OK' } as any;

    mockHealthService.getHealthSnapshot.mockReturnValue(of(mockResponse));

    component.healthData$.subscribe((data) => {
      expect(data).toEqual(mockResponse);
      expect(mockHealthService.getHealthSnapshot).toHaveBeenCalled();
      done();
    });

    mockRefreshIntervalService.intervalData$.next();
  });

  // --------------------------------------------------
  // refreshIntervalObs - error case (catchError → EMPTY)
  // --------------------------------------------------

  it('should return EMPTY when healthService throws error', (done) => {
    mockHealthService.getHealthSnapshot.mockReturnValue(throwError(() => new Error('API Error')));

    let emitted = false;

    component.healthData$.subscribe({
      next: () => {
        emitted = true;
      },
      complete: () => {
        expect(emitted).toBe(false);
        done();
      }
    });

    mockRefreshIntervalService.intervalData$.next();
    mockRefreshIntervalService.intervalData$.complete();
  });

  // --------------------------------------------------
  // refreshIntervalObs - exhaustMap behavior
  // --------------------------------------------------

  it('should ignore new interval emissions until previous completes', () => {
    const interval$ = new Subject<void>();
    const inner$ = new Subject<any>();

    const mockRefreshService = {
      intervalData$: interval$
    };

    const testComponent = new OverviewComponent(
      mockHealthService as any,
      mockRefreshService as any
    );

    mockHealthService.getHealthSnapshot.mockReturnValue(inner$);

    testComponent.healthData$.subscribe();

    // First emission
    interval$.next();

    // Second emission (should be ignored)
    interval$.next();

    expect(mockHealthService.getHealthSnapshot).toHaveBeenCalledTimes(1);

    // Complete first inner observable
    inner$.complete();

    // Now it should allow another call
    interval$.next();

    expect(mockHealthService.getHealthSnapshot).toHaveBeenCalledTimes(2);
  });

  // --------------------------------------------------
  // ngOnDestroy
  // --------------------------------------------------

  it('should complete destroy$ on destroy', () => {
    const nextSpy = jest.spyOn((component as any).destroy$, 'next');
    const completeSpy = jest.spyOn((component as any).destroy$, 'complete');

    component.ngOnDestroy();

    expect(nextSpy).toHaveBeenCalled();
    expect(completeSpy).toHaveBeenCalled();
  });

  // --------------------------------------------------
  // refreshIntervalObs manual test
  // --------------------------------------------------

  it('refreshIntervalObs should pipe intervalData$', (done) => {
    const testFn = jest.fn().mockReturnValue(of('TEST'));

    const obs$ = component.refreshIntervalObs(testFn);

    obs$.subscribe((value) => {
      expect(value).toBe('TEST');
      expect(testFn).toHaveBeenCalled();
      done();
    });

    mockRefreshIntervalService.intervalData$.next();
  });
});
