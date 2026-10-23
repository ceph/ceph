import { SharedModule } from '~/app/shared/shared.module';
import { RgwRateLimitDetailsComponent } from './rgw-rate-limit-details.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { USER } from '~/app/shared/constants/app.constants';
import { RgwRateLimitConfig } from '../models/rgw-rate-limit';

describe('RgwRateLimitDetailsComponent', () => {
  let component: RgwRateLimitDetailsComponent;
  let fixture: ComponentFixture<RgwRateLimitDetailsComponent>;
  configureTestBed({
    declarations: [RgwRateLimitDetailsComponent],
    imports: [HttpClientTestingModule, SharedModule]
  });

  const baseConfig: RgwRateLimitConfig = {
    enabled: true,
    max_read_bytes: 987648,
    max_read_ops: 0,
    max_write_bytes: 0,
    max_write_ops: 9876543212,
    max_list_ops: 40,
    max_delete_ops: 10
  };

  const render = (type: string, config: RgwRateLimitConfig) => {
    component.type = type;
    component.rateLimitConfig = config;
    fixture.detectChanges();
  };

  const getRowValue = (label: string): string => {
    const rows: HTMLTableRowElement[] = Array.from(
      fixture.nativeElement.querySelectorAll('tbody tr')
    );
    const row = rows.find((r) => r.querySelector('td').textContent.trim() === label);
    return row ? row.querySelectorAll('td')[1].textContent.trim() : undefined;
  };

  const getRowLabels = (): string[] =>
    Array.from(fixture.nativeElement.querySelectorAll('tbody tr')).map((r: HTMLTableRowElement) =>
      r.querySelector('td').textContent.trim()
    );

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwRateLimitDetailsComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    render(USER, baseConfig);
    expect(component).toBeTruthy();
  });

  it('should render list and delete ops rows right after write ops', () => {
    render(USER, baseConfig);
    expect(getRowLabels()).toEqual([
      'Enabled',
      'Maximum Read Ops',
      'Maximum Write Ops',
      'Maximum List Ops',
      'Maximum Delete Ops',
      'Maximum Read Bytes',
      'Maximum Write Bytes'
    ]);
  });

  it('should explain dual throttling in the list and delete tooltips', () => {
    render(USER, baseConfig);
    const title = (label: string): string => {
      const rows: HTMLTableRowElement[] = Array.from(
        fixture.nativeElement.querySelectorAll('tbody tr')
      );
      const row = rows.find((r) => r.querySelector('td').textContent.trim() === label);
      return row.querySelector('td').getAttribute('title');
    };
    expect(title('Maximum List Ops')).toContain('maximum read ops');
    expect(title('Maximum Delete Ops')).toContain('maximum write ops');
    expect(title('Maximum Delete Ops')).toContain('S3 multi-object delete is not rate limited');
    expect(title('Maximum Read Ops')).toBeNull();
  });

  [USER, 'bucket'].forEach((type) => {
    describe(`${type} details`, () => {
      it('should show configured list and delete thresholds', () => {
        render(type, baseConfig);
        expect(getRowValue('Maximum List Ops')).toBe('40');
        expect(getRowValue('Maximum Delete Ops')).toBe('10');
      });

      it('should show Unlimited when list and delete ops are 0', () => {
        render(type, { ...baseConfig, max_list_ops: 0, max_delete_ops: 0 });
        expect(getRowValue('Maximum List Ops')).toBe('Unlimited');
        expect(getRowValue('Maximum Delete Ops')).toBe('Unlimited');
      });

      it('should show Unlimited when list and delete ops are missing', () => {
        const legacy: any = { ...baseConfig };
        delete legacy.max_list_ops;
        delete legacy.max_delete_ops;
        render(type, legacy);
        expect(getRowValue('Maximum List Ops')).toBe('Unlimited');
        expect(getRowValue('Maximum Delete Ops')).toBe('Unlimited');
      });

      it('should show - when rate limiting is disabled', () => {
        render(type, { ...baseConfig, enabled: false });
        expect(getRowValue('Maximum List Ops')).toBe('-');
        expect(getRowValue('Maximum Delete Ops')).toBe('-');
      });
    });
  });
});
