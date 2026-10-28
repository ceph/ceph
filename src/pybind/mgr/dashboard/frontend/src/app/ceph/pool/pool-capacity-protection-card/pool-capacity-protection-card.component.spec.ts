import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { PoolOverviewModel } from '~/app/shared/models/pool-overview.model';
import { PoolCapacityProtectionCardComponent } from './pool-capacity-protection-card.component';

describe('PoolCapacityProtectionCardComponent', () => {
  let component: PoolCapacityProtectionCardComponent;
  let fixture: ComponentFixture<PoolCapacityProtectionCardComponent>;
  const baseOverviewModel: PoolOverviewModel = {
    name: 'test-pool',
    type: 'replicated',
    dataProtection: 'replica: x3',
    applications: ['rbd'],
    pgStatus: 'active+clean',
    crushRuleset: 'replicated_rule',
    usageTotal: 1000,
    usageUsed: 250,
    usagePercent: '25%',
    usedCapacity: '250 B',
    availableCapacity: '750 B',
    totalCapacity: '1000 B',
    quotaLimit: 'No quota',
    isErasure: false,
    typeLabel: 'replicated',
    replicationSize: '3',
    minSize: '2',
    erasureK: '',
    erasureM: '',
    erasureTotal: '',
    erasurePlugin: '',
    failureDomain: 'host',
    readThroughput: '0 B/s',
    readOps: '0/s',
    readOpsChartData: [],
    writeThroughput: '0 B/s',
    writeOps: '0/s',
    writeOpsChartData: []
  };

  configureTestBed({
    imports: [SharedModule],
    declarations: [PoolCapacityProtectionCardComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolCapacityProtectionCardComponent);
    component = fixture.componentInstance;
    component.overviewModel = { ...baseOverviewModel };
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('Capacity Section', () => {
    it('should display capacity details and usage bar when usage data is available', () => {
      fixture.detectChanges();
      const textContent = fixture.nativeElement.textContent;

      expect(textContent).toContain('Capacity');
      expect(textContent).toContain('25%');
      expect(textContent).toContain('250 B');
      expect(textContent).toContain('750 B');
      expect(textContent).toContain('1000 B');
      expect(textContent).toContain('No quota');
      const usageBar = fixture.nativeElement.querySelector('cd-usage-bar');
      expect(usageBar).toBeTruthy();
    });

    it('should hide the usage bar and show a dash if usageTotal is 0', () => {
      component.overviewModel = { ...baseOverviewModel, usageTotal: 0 };
      fixture.detectChanges();

      const usageBar = fixture.nativeElement.querySelector('cd-usage-bar');
      expect(usageBar).toBeFalsy();
    });

    it('should hide the usage bar and show a dash if usageUsed is null', () => {
      component.overviewModel = { ...baseOverviewModel, usageUsed: null };
      fixture.detectChanges();

      const usageBar = fixture.nativeElement.querySelector('cd-usage-bar');
      expect(usageBar).toBeFalsy();
    });
  });

  describe('Data Protection Section', () => {
    it('should display common fields (CRUSH, Failure Domain, Type)', () => {
      fixture.detectChanges();
      const textContent = fixture.nativeElement.textContent;

      expect(textContent).toContain('replicated');
      expect(textContent).toContain('replicated_rule');
      expect(textContent).toContain('host');
    });

    it('should display replicated specific fields when pool is NOT erasure coded', () => {
      component.overviewModel = { ...baseOverviewModel, isErasure: false };
      fixture.detectChanges();

      const textContent = fixture.nativeElement.textContent;

      expect(textContent).toContain('Replication size');
      expect(textContent).toContain('3');
      expect(textContent).toContain('Min size');
      expect(textContent).toContain('2');

      // Ensure erasure fields are NOT rendered
      expect(textContent).not.toContain('K (Split)');
      expect(textContent).not.toContain('M (Chunks)');
    });

    it('should display erasure coded specific fields when pool is erasure coded', () => {
      component.overviewModel = {
        ...baseOverviewModel,
        isErasure: true,
        typeLabel: 'Erasure Coded',
        erasureK: '4',
        erasureM: '2',
        erasureTotal: '6',
        erasurePlugin: 'jerasure'
      };
      fixture.detectChanges();

      const textContent = fixture.nativeElement.textContent;

      expect(textContent).toContain('Erasure Coded');
      expect(textContent).toContain('K (Split)');
      expect(textContent).toContain('4');
      expect(textContent).toContain('M (Chunks)');
      expect(textContent).toContain('2');
      expect(textContent).toContain('Plugin');
      expect(textContent).toContain('jerasure');
      expect(textContent).not.toContain('Replication size');
    });
  });
});
