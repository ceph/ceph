import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsDetailComponent } from './cephfs-detail.component';

@Component({ selector: 'cd-cephfs-chart', template: '' })
class CephfsChartStubComponent {
  @Input()
  mdsCounter: any;
}

describe('CephfsDetailComponent', () => {
  let component: CephfsDetailComponent;
  let fixture: ComponentFixture<CephfsDetailComponent>;

  const updateDetails = (standbys, pools, ranks, mdsCounters, name) => {
    component.data = {
      standbys,
      pools,
      ranks,
      mdsCounters,
      name
    };
    fixture.detectChanges();
  };

  configureTestBed({
    imports: [SharedModule],
    declarations: [CephfsDetailComponent, CephfsChartStubComponent],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsDetailComponent);
    component = fixture.componentInstance;
    updateDetails('b', [], [], { a: { name: 'a', x: [0], y: [0, 1] } }, 'someFs');
    fixture.detectChanges();
    component.ngOnChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('prepares standby on change', () => {
    expect(component.standbys).toEqual([{ key: 'Standby daemons', value: 'b' }]);
  });
});
