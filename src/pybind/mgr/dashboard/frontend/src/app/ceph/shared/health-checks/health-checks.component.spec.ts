import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HealthChecksComponent } from './health-checks.component';
import { HealthColorPipe } from '~/app/shared/pipes/health-color.pipe';
import { By } from '@angular/platform-browser';
import { CssHelper } from '~/app/shared/classes/css-helper';

describe('HealthChecksComponent', () => {
  let component: HealthChecksComponent;
  let fixture: ComponentFixture<HealthChecksComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [HealthChecksComponent, HealthColorPipe],
      providers: [CssHelper]
    }).compileComponents();

    fixture = TestBed.createComponent(HealthChecksComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show the correct health warning for failed daemons', () => {
    component.healthData = [
      {
        severity: 'HEALTH_WARN',
        summary: {
          message: '1 failed cephadm daemon(s)',
          count: 1
        },
        detail: [
          {
            message: 'daemon ceph-exporter.ceph-node-00 on ceph-node-00 is in error state'
          }
        ],
        muted: false,
        type: 'CEPHADM_FAILED_DAEMON'
      }
    ];
    fixture.detectChanges();
    const failedDaemons = fixture.debugElement.query(By.css('.failed-daemons'));
    expect(failedDaemons.nativeElement.textContent).toContain(
      'Failed Daemons: ceph-exporter.ceph-node-00  '
    );
  });
});
