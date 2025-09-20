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
          '--cds-support-info': '#00f',
          '--cds-support-warning': '#ff0',
          '--cds-support-danger': '#f00'
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

  it('should return the correct tooltip string for defaultTooltip', () => {
    component.total = 100000;
    component.used = 40000;
    component.isBinary = false;
    component.decimals = 2;
    const normalize = (str: string) => str.replace(/\s+/g, ' ').trim();
    const tooltip = component['defaultTooltip']();
    expect(normalize(tooltip)).toContain(
      normalize(`<div class="meter-tooltip">
    <div><strong>Used:</strong> 40 k</div>
    <div><strong>Available:</strong> 60 k</div>
    <div><strong>Total:</strong> 100 k</div>
  </div>`)
    );
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
