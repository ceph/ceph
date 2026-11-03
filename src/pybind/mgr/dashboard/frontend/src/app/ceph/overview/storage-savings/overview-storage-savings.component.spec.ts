import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AreaChartComponent } from '~/app/shared/components/area-chart/area-chart.component';
import { SavingsCardVM } from '~/app/shared/models/overview';
import { OverviewStorageSavingsComponent } from './overview-storage-savings.component';

const createSavingsVm = (overrides: Partial<SavingsCardVM> = {}): SavingsCardVM => ({
  total: '7 TiB',
  trendData: [{ timestamp: new Date('2024-01-01T00:00:00Z'), values: { Used: 1 } }],
  isTrendingUp: true,
  stats: [{ label: 'Compression', value: '2.4', description: 'ratio' }],
  ...overrides
});

describe('OverviewStorageSavingsComponent', () => {
  let fixture: ComponentFixture<OverviewStorageSavingsComponent>;
  let element: HTMLElement;

  const render = (model: SavingsCardVM) => {
    fixture = TestBed.createComponent(OverviewStorageSavingsComponent);
    fixture.componentRef.setInput('savings', model);
    element = fixture.nativeElement;
    fixture.detectChanges();
  };

  const totalSavingsText = () =>
    element.querySelector('.savings-header')?.children[1].textContent?.trim();

  beforeEach(() =>
    TestBed.configureTestingModule({ imports: [OverviewStorageSavingsComponent] })
      .overrideComponent(OverviewStorageSavingsComponent, {
        remove: { imports: [AreaChartComponent] },
        add: { schemas: [CUSTOM_ELEMENTS_SCHEMA] }
      })
      .compileComponents()
  );

  it('should render the total, the trend chart and the stats', () => {
    render(createSavingsVm());

    expect(totalSavingsText()).toBe('7 TiB');
    expect(element.querySelector('cd-area-chart')).not.toBeNull();
    expect(element.querySelector('cd-overview-stats')?.textContent).toContain('Compression');
  });

  it('should show "Not available" when there is no total', () => {
    render(createSavingsVm({ total: null, isTrendingUp: false }));

    expect(totalSavingsText()).toBe('Not available');
  });

  it('should show the empty message when there is no trend data', () => {
    render(createSavingsVm({ trendData: [] }));

    expect(element.querySelector('cd-area-chart')).toBeNull();
    expect(element.textContent).toContain('No trend data available.');
  });

  it.each([
    [true, true],
    [false, false]
  ])('should show the growth icon only when trending up (%s)', (isTrendingUp, shown) => {
    render(createSavingsVm({ isTrendingUp }));

    expect(!!element.querySelector('cd-icon[type="growth"]')).toBe(shown);
  });
});
