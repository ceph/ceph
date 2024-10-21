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

    component.data = [{ group: 'Capacity', value: 0 }];
    component.options = {
      resizable: false,
      meter: { showLabels: true },
      tooltip: { enabled: true },
      color: { scale: { Capacity: '' } },
      height: '100%',
      width: '100%',
      toolbar: { enabled: false }
    };

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

  it('should set color to info if under thresholds', () => {
    component.used = 30;
    component.total = 100;
    component.warningThreshold = 0.5;
    component.errorThreshold = 0.8;

    component.ngOnInit();

    expect(component.data[0].value).toBe(0.3);
    expect((component.options.color.scale as { [key: string]: string })['Capacity'].trim()).toBe(
      '#00f'
    );
  });

  it('should calculate used percentage correctly', () => {
    expect(component.calculateUsed(50, 100)).toBe(0.5);
    expect(component.calculateUsed(0, 0)).toBe(0);
  });

  it('should get correct CSS variable value', () => {
    const value = component.getCssVariableValue('cds-support-info');
    expect(value.trim()).toBe('#00f');
  });
});
