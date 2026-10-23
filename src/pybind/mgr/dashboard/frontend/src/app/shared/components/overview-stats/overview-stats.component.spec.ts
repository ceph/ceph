import { ComponentFixture, TestBed } from '@angular/core/testing';

import { OverviewStat } from '~/app/shared/models/overview';
import { OverviewStatsComponent } from './overview-stats.component';

describe('OverviewStatsComponent', () => {
  let fixture: ComponentFixture<OverviewStatsComponent>;
  let element: HTMLElement;

  const render = (stats: OverviewStat[]) => {
    fixture = TestBed.createComponent(OverviewStatsComponent);
    fixture.componentRef.setInput('stats', stats);
    element = fixture.nativeElement;
    fixture.detectChanges();
  };

  const renderedValues = () =>
    Array.from(element.querySelectorAll('.cds--type-heading-03')).map((el) =>
      el.textContent?.trim()
    );
  const labelCount = () => element.querySelectorAll('cds-tooltip-definition').length;

  beforeEach(() => TestBed.configureTestingModule({ imports: [OverviewStatsComponent] }));

  it('should render each stat value with its label', () => {
    render([
      { label: 'Compression', value: '2.4', description: 'ratio' },
      { label: 'Efficiency', value: '58.3%', description: 'saved' }
    ]);

    expect(renderedValues()).toEqual(['2.4', '58.3%']);
    expect(labelCount()).toBe(2);
    expect(element.textContent).toContain('Compression');
    expect(element.textContent).toContain('Efficiency');
  });

  it('should show a muted "Not available" instead of a missing value', () => {
    render([{ label: 'Compression', value: null, description: 'ratio' }]);

    expect(renderedValues()).toEqual([]);
    expect(element.querySelector('.overview-stats-unavailable')?.textContent?.trim()).toBe(
      'Not available'
    );
    expect(element.textContent).toContain('Compression');
  });

  it('should render nothing for an empty list', () => {
    render([]);

    expect(labelCount()).toBe(0);
  });
});
