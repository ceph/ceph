import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { UsageBarComponent } from './usage-bar.component';
import { ElementRef } from '@angular/core';
import { CssHelper } from '../../classes/css-helper';

const mockElementRef = {
  nativeElement: {}
};

describe('UsageBarComponent', () => {
  let component: UsageBarComponent;
  let fixture: ComponentFixture<UsageBarComponent>;

  configureTestBed({
    imports: [PipesModule, NgbTooltipModule],
    declarations: [UsageBarComponent],
    providers: [{ provide: ElementRef, useValue: mockElementRef }, CssHelper]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UsageBarComponent);
    component = fixture.componentInstance;

    window.getComputedStyle = jest.fn().mockReturnValue({
      getPropertyValue: (name: string) => {
        const mockStyles: Record<string, string> = {
          '--cds-support-info': '#00f'
        };
        return mockStyles[name] || '';
      }
    });

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should get correct CSS variable value', () => {
    const value = component['getCssVariableValue']('cds-support-info');
    expect(value.trim()).toBe('#00f');
  });

  it('should set correct thresholds for proportional usage', () => {
    component.total = 100;
    component.used = 30;
    component.warningThreshold = 0.5;
    component.errorThreshold = 0.8;
    component.proportional = true;

    component.ngOnInit();

    const ranges = component.options.meter.status.ranges;
    expect(ranges.length).toBe(3);
    expect(ranges[0].range).toEqual([0, 50]);
    expect(ranges[1].range).toEqual([50, 80]);
    expect(ranges[2].range).toEqual([80, 100]);
  });

  it('should set correct thresholds for non-proportional usage', () => {
    component.total = 100;
    component.used = 30;
    component.warningThreshold = 0.5;
    component.errorThreshold = 0.8;
    component.proportional = false;

    component.ngOnInit();

    const ranges = component.options.meter.status.ranges;
    expect(ranges[0].range).toEqual([0, 50]);
    expect(ranges[1].range).toEqual([50, 80]);
    expect(ranges[2].range).toEqual([80, 100]);
  });

  it('should format total and breakdown correctly for binary usage', () => {
    component.total = 1024;
    component.used = 512;
    component.isBinary = true;
    component.decimals = 2;
    component.proportional = true;

    component.ngOnInit();

    const totalFormatted = component.options.meter.proportional?.totalFormatter?.(component.total);
    const breakdownFormatted = component.options.meter.proportional?.breakdownFormatter?.(
      component.used
    );

    expect(totalFormatted).toContain('KiB total');
    expect(breakdownFormatted).toContain('used');
  });

  it('should format total and breakdown correctly for decimal usage', () => {
    component.total = 100000;
    component.used = 30000;
    component.isBinary = false;
    component.decimals = 2;
    component.proportional = true;

    component.ngOnInit();

    const totalFormatted = component.options.meter.proportional?.totalFormatter?.(component.total);
    const breakdownFormatted = component.options.meter.proportional?.breakdownFormatter?.(
      component.used
    );

    expect(totalFormatted).toContain('k total');
    expect(breakdownFormatted).toContain('used');
  });
  it('should return the correct tooltip string for defaultTooltip', () => {
    component.total = 100000;
    component.used = 40000;
    component.isBinary = false;
    component.decimals = 2;

    const tooltip = component['defaultTooltip']();
    expect(tooltip).toContain('40 k used (60 k available) &nbsp;&nbsp; 100 k total');
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

  it('should return default breakdown format when customBreakdownFormatter is not provided', () => {
    component.used = 30000;
    component.total = 100000;
    component.customBreakdownFormatter = undefined;
    const breakdownFormatted = component['getBreakdownFormatter']();
    expect(breakdownFormatted).toContain('29.3 KiB used');
  });
  it('should return custom total format when customTotalFormatter is provided', () => {
    component.customTotalFormatter = jest.fn().mockReturnValue('Custom total format');
    const totalFormatted = component['getTotalFormatter']();
    expect(totalFormatted).toBe('Custom total format');
    expect(component.customTotalFormatter).toHaveBeenCalledWith(component.total);
  });

  it('should return default total format when customTotalFormatter is not provided', () => {
    component.total = 100000;
    component.customTotalFormatter = undefined;
    const totalFormatted = component['getTotalFormatter']();
    expect(totalFormatted).toContain('97.66 KiB total');
  });
});
