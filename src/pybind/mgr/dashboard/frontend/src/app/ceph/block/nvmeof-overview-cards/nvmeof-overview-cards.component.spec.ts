import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { NvmeofOverviewCardsComponent } from './nvmeof-overview-cards.component';

describe('NvmeofOverviewCardsComponent', () => {
  let component: NvmeofOverviewCardsComponent;
  let fixture: ComponentFixture<NvmeofOverviewCardsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofOverviewCardsComponent],
      imports: [ProductiveCardComponent],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofOverviewCardsComponent);
    component = fixture.componentInstance;
    component.stats = {
      gatewayGroups: 5,
      subsystems: 5,
      namespaces: 13,
      hosts: 15,
      activeConnections: 24
    };
    component.alerts$ = of({
      critical: 0,
      warning: 0,
      total: 0,
      byCategory: {}
    });
    component.throughput$ = of({
      reads: 2512.98,
      writes: 1897.92,
      combined: 4153
    });
    component.alertQueryParams = jest.fn().mockImplementation((severity: string, category?: string) => {
      const params: Record<string, string> = { severity };
      if (category) {
        params.category = category;
      }
      return params;
    });
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render all default card headers and key metrics', () => {
    const text = fixture.nativeElement.textContent;

    expect(text).toContain('Resources status');
    expect(text).toContain('Alert notifications');
    expect(text).toContain('Throughput');
    expect(text).toContain('Gateway groups');
    expect(text).toContain('5');
    expect(text).toContain('Reads');
    expect(text).toContain('2,512.98 MB/s');
    expect(text).toContain('Active connections: 24');
  });

  it('should render alert category rows when alerts exist', () => {
    component.alerts$ = of({
      critical: 1,
      warning: 1,
      total: 2,
      byCategory: {
        gateway: 1,
        listener: 1
      }
    });

    fixture.detectChanges();

    const text = fixture.nativeElement.textContent;
    const categoryRows = fixture.nativeElement.querySelectorAll('.nvmeof-alerts-card__category-row');

    expect(text).toContain('Need attention');
    expect(text).toContain('Gateway');
    expect(text).toContain('Listener');
    expect(categoryRows.length).toBe(2);
  });

  it('should support dynamic card and row configuration via inputs', () => {
    component.overviewCards = [{ id: 'throughput', title: 'Throughput only' }] as any;
    component.throughputRows = [{ key: 'writes', label: 'Writes only' }] as any;

    fixture.detectChanges();

    const text = fixture.nativeElement.textContent;

    expect(text).toContain('Throughput only');
    expect(text).toContain('Writes only');
    expect(text).not.toContain('Resources status');
    expect(text).not.toContain('Alert notifications');
    expect(text).not.toContain('Reads:');
  });
});
