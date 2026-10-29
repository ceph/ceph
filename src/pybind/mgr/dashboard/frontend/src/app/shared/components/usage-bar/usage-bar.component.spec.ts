import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { TooltipModule } from 'carbon-components-angular';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { UsageBarComponent } from './usage-bar.component';

describe('UsageBarComponent', () => {
  let component: UsageBarComponent;
  let fixture: ComponentFixture<UsageBarComponent>;

  configureTestBed({
    imports: [PipesModule, TooltipModule],
    declarations: [UsageBarComponent]
  });

  const render = (inputs: Partial<UsageBarComponent>) => {
    fixture = TestBed.createComponent(UsageBarComponent);
    component = fixture.componentInstance;
    Object.entries(inputs).forEach(([key, value]) => fixture.componentRef.setInput(key, value));
    fixture.detectChanges();
    return fixture.nativeElement as HTMLElement;
  };
  const query = (el: HTMLElement, testId: string) =>
    el.querySelector(`[data-testid="${testId}"]`) as HTMLElement;

  beforeEach(() => {
    render({ total: 100, used: 0 });
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render the used part of the bar and the percentage', () => {
    const el = render({ total: 200, used: 50 });
    expect(query(el, 'usage-bar-fill').style.width).toBe('25%');
    expect(query(el, 'usage-bar-breakdown').textContent.trim()).toBe('25.00%');
    expect(query(el, 'usage-bar-total')).toBeNull();
  });

  it('should expose the usage as a meter', () => {
    const el = render({ total: 200, used: 50, title: 'osd.1' });
    const meter = el.querySelector('[role="meter"]');
    expect(meter.getAttribute('aria-valuenow')).toBe('25');
    expect(meter.getAttribute('aria-valuetext')).toBe('25.00%');
    expect(meter.getAttribute('aria-label')).toBe('osd.1');
  });

  it('should clamp the bar width to 100%', () => {
    const el = render({ total: 100, used: 150 });
    expect(query(el, 'usage-bar-fill').style.width).toBe('100%');
  });

  it('should show the no data text if total is not known', () => {
    const el = render({ total: 0, used: 0, noDataText: 'n/a' });
    expect(query(el, 'usage-bar')).toBeNull();
    expect(el.textContent.trim()).toBe('n/a');
  });

  it('should update when inputs change', () => {
    const el = render({ total: 100, used: 10 });
    fixture.componentRef.setInput('used', 60);
    fixture.detectChanges();
    expect(query(el, 'usage-bar-fill').style.width).toBe('60%');
    expect(query(el, 'usage-bar-breakdown').textContent.trim()).toBe('60.00%');
  });

  it('should replace the labels with a custom label', () => {
    const el = render({ total: 100, used: 30, customLabel: 'Capacity' });
    expect(el.textContent).toContain('Capacity');
    expect(query(el, 'usage-bar-breakdown')).toBeNull();
    expect(query(el, 'usage-bar-fill').style.width).toBe('30%');
  });

  it('should show the tooltip with the values on hover', fakeAsync(() => {
    const el = render({ total: 100000, used: 40000, isBinary: false });
    const trigger = query(el, 'usage-bar-tooltip');
    trigger.dispatchEvent(new Event('mouseenter'));
    tick(100); // Carbon tooltip enter delay
    fixture.detectChanges();
    expect(trigger.classList).toContain('cds--popover--open');
    expect(
      trigger.querySelector('.cds--popover-content').textContent.replace(/\s+/g, ' ')
    ).toContain('Used: 40 k');
    trigger.dispatchEvent(new Event('mouseleave'));
    tick(300); // Carbon tooltip leave delay
    fixture.detectChanges();
    expect(trigger.classList).not.toContain('cds--popover--open');
  }));

  it('should provide the values for the tooltip', () => {
    render({ total: 100000, used: 40000, isBinary: false });
    expect(component.usedText).toBe('40 k');
    expect(component.availableText).toBe('60 k');
    expect(component.totalText).toBe('100 k');
  });

  describe('thresholds', () => {
    const renderWithThresholds = (used: number) =>
      render({ total: 100, used, warningThreshold: 0.85, errorThreshold: 0.95 });

    it.each([
      [50, 'normal'],
      [85, 'warning'],
      [95, 'danger'],
      [100, 'danger'],
      [110, 'danger']
    ])('should set the status (%s %% used => %s)', (used: number, status: string) => {
      const el = renderWithThresholds(used);
      const fill = query(el, 'usage-bar-fill');
      const indicator = query(el, 'usage-bar-status');
      expect(component.status).toBe(status);
      expect(fill.classList.contains('usage-bar__fill--warning')).toBe(status === 'warning');
      expect(fill.classList.contains('usage-bar__fill--danger')).toBe(status === 'danger');
      if (status === 'normal') {
        expect(indicator).toBeNull();
      } else {
        expect(indicator.classList).toContain(`usage-bar__status--${status}`);
      }
    });

    it('should ignore unknown thresholds', () => {
      const el = render({ total: 100, used: 99, warningThreshold: -1, errorThreshold: -1 });
      expect(component.status).toBe('normal');
      expect(query(el, 'usage-bar-status')).toBeNull();
    });

    it('should apply thresholds that are set later', () => {
      const el = render({ total: 100, used: 99 });
      expect(component.status).toBe('normal');
      fixture.componentRef.setInput('warningThreshold', 0.85);
      fixture.componentRef.setInput('errorThreshold', 0.95);
      fixture.detectChanges();
      expect(component.status).toBe('danger');
      expect(query(el, 'usage-bar-status')).not.toBeNull();
    });
  });

  it('should return custom breakdown format when customBreakdownFormatter is provided', () => {
    component.customBreakdownFormatter = jest.fn().mockReturnValue('Custom breakdown format');
    const breakdownFormatted = component['getBreakdownFormatter']();
    expect(breakdownFormatted).toBe('Custom breakdown format');
    expect(component.customBreakdownFormatter).toHaveBeenCalledWith({
      used: component.used,
      total: component.total
    });
  });

  it('should return custom total format when customTotalFormatter is provided', () => {
    component.customTotalFormatter = jest.fn().mockReturnValue('Custom total format');
    const totalFormatted = component['getTotalFormatter']();
    expect(totalFormatted).toBe('Custom total format');
    expect(component.customTotalFormatter).toHaveBeenCalledWith(component.total);
  });

  describe('when enablePercentageLabel is true', () => {
    beforeEach(() => {
      component.total = 200;
      component.used = 50;
      component.decimals = 2;
      component.enablePercentageLabel = true;
    });

    it('should return empty total formatter', () => {
      const result = component['getTotalFormatter']();
      expect(result).toBe('');
    });

    it('should return percentage breakdown formatter', () => {
      const result = component['getBreakdownFormatter']();
      expect(result).toBe('25.00%');
    });

    it('should return "0%" when used is 0', () => {
      component.used = 0;
      component.total = 100;
      expect(component['getBreakdownFormatter']()).toBe('0%');
    });

    it('should clamp small percentage to 0% if less than 0.01%, otherwise format normally', () => {
      component.total = 100000000;

      component.used = 5; // 0.000005 => 0.0005%
      expect(component['getBreakdownFormatter']()).toBe('0%');

      component.used = 10000; // 10000 / 100000000 = 0.01%
      expect(component['getBreakdownFormatter']()).toBe('0.01%');

      component.used = 500000; // 0.5%
      expect(component['getBreakdownFormatter']()).toBe('0.50%');
    });
  });

  describe('when enablePercentageLabel is false', () => {
    beforeEach(() => {
      component.total = 10242424;
      component.used = 5122424;
      component.enablePercentageLabel = false;
      component.decimals = 2;
      component.isBinary = true;
    });

    it('should render used and total labels', () => {
      const el = render({
        total: 10242424,
        used: 5122424,
        enablePercentageLabel: false
      });
      expect(query(el, 'usage-bar-breakdown').textContent.trim()).toBe('4.89 MiB used');
      expect(query(el, 'usage-bar-total').textContent.trim()).toBe('9.77 MiB total');
    });

    it('should return total with formatted binary value', () => {
      const result = component['getTotalFormatter']();
      expect(result).toContain('9.77 MiB total');
    });

    it('should return used with formatted binary value in breakdown', () => {
      const result = component['getBreakdownFormatter']();
      expect(result).toContain('used');
      expect(result).toContain('4.89 MiB used');
    });

    it('should return default total format when customTotalFormatter is not provided', () => {
      component.total = 100000;
      component.customTotalFormatter = undefined;
      const totalFormatted = component['getTotalFormatter']();
      expect(totalFormatted).toContain('97.66 KiB total');
    });

    it('should return default breakdown format when customBreakdownFormatter is not provided', () => {
      component.used = 30000;
      component.total = 100000;
      component.customBreakdownFormatter = undefined;
      const breakdownFormatted = component['getBreakdownFormatter']();
      expect(breakdownFormatted).toContain('29.3 KiB used');
    });
  });
});
