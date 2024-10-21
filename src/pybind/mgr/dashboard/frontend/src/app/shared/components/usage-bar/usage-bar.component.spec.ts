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

    const totalFormatted = component.options.meter.proportional?.totalFormatter?.();
    const breakdownFormatted = component.options.meter.proportional?.breakdownFormatter?.();

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

    const totalFormatted = component.options.meter.proportional?.totalFormatter?.();
    const breakdownFormatted = component.options.meter.proportional?.breakdownFormatter?.();

    expect(totalFormatted).toContain('k total');
    expect(breakdownFormatted).toContain('used');
  });
});
