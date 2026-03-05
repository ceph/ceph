import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BehaviorSubject } from 'rxjs';

import { OverviewAlertsCardComponent } from './overview-alerts-card.component';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { provideHttpClient } from '@angular/common/http';
import { provideRouter, RouterModule } from '@angular/router';
import { take } from 'rxjs/operators';

class MockPrometheusAlertService {
  private totalSub = new BehaviorSubject<number>(0);
  private criticalSub = new BehaviorSubject<number>(0);
  private warningSub = new BehaviorSubject<number>(0);

  totalAlerts$ = this.totalSub.asObservable();
  criticalAlerts$ = this.criticalSub.asObservable();
  warningAlerts$ = this.warningSub.asObservable();

  getGroupedAlerts = jest.fn();

  emitCounts(total: number, critical: number, warning: number) {
    this.totalSub.next(total);
    this.criticalSub.next(critical);
    this.warningSub.next(warning);
  }
}

describe('OverviewAlertsCardComponent', () => {
  let component: OverviewAlertsCardComponent;
  let fixture: ComponentFixture<OverviewAlertsCardComponent>;
  let mockSvc: MockPrometheusAlertService;

  beforeEach(async () => {
    mockSvc = new MockPrometheusAlertService();

    await TestBed.configureTestingModule({
      imports: [OverviewAlertsCardComponent, RouterModule],
      providers: [
        provideRouter([]),
        provideHttpClient(),
        { provide: PrometheusAlertService, useValue: mockSvc }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewAlertsCardComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('ngOnInit should call getGroupedAlerts(true)', () => {
    fixture.detectChanges();
    expect(mockSvc.getGroupedAlerts).toHaveBeenCalledWith(true);
  });

  it('vm$ should map no alerts -> success icon, "No active alerts", no badges', async () => {
    mockSvc.emitCounts(0, 0, 0);
    fixture.detectChanges();

    const vm = await component.vm$.pipe(take(1)).toPromise();

    expect(vm.total).toBe(0);
    expect(vm.icon).toBe('success');
    expect(vm.statusText).toBe('No active alerts');
    expect(vm.badges).toEqual([]);
  });

  it('vm$ should map critical alerts -> error icon and critical badge', async () => {
    mockSvc.emitCounts(5, 2, 3);
    fixture.detectChanges();

    const vm = await component.vm$.pipe(take(1)).toPromise();

    expect(vm.total).toBe(5);
    expect(vm.icon).toBe('error');
    expect(vm.statusText).toBe('Need attention');

    expect(vm.badges).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ key: 'critical', icon: 'error', count: 2 }),
        expect.objectContaining({ key: 'warning', icon: 'warning', count: 3 })
      ])
    );
  });

  it('vm$ should map warning-only -> warning icon and warning badge only', async () => {
    mockSvc.emitCounts(3, 0, 3);
    fixture.detectChanges();

    const vm = await component.vm$.pipe(take(1)).toPromise();

    expect(vm.total).toBe(3);
    expect(vm.icon).toBe('warning');
    expect(vm.statusText).toBe('Need attention');

    expect(vm.badges).toEqual([{ key: 'warning', icon: 'warning', count: 3 }]);
  });

  it('template should render border class only on 2nd badge (when both exist)', async () => {
    mockSvc.emitCounts(10, 1, 2);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();

    const badgeEls = Array.from(
      fixture.nativeElement.querySelectorAll('.overview-alerts-card-badge')
    ) as HTMLElement[];

    expect(badgeEls.length).toBe(2);
    expect(badgeEls[0].classList.contains('overview-alerts-card-badge-with-border--right')).toBe(
      true
    );
    expect(badgeEls[1].classList.contains('overview-alerts-card-badge-with-border--right')).toBe(
      false
    );
  });
});
