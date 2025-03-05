import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbClusterTabsComponent } from './smb-cluster-tabs.component';
import { CLUSTER_RESOURCE, SMBCluster } from '../smb.model';
import { By } from '@angular/platform-browser';

describe('SmbClusterTabsComponent', () => {
  let component: SmbClusterTabsComponent;
  let fixture: ComponentFixture<SmbClusterTabsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbClusterTabsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbClusterTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should not render anything if selection is falsy', () => {
    component.selection = null;
    fixture.detectChanges();

    const tabsElement = fixture.debugElement.query(By.css('cds-tabs'));
    expect(tabsElement).toBeNull();
  });

  const selectedSmbCluster = (clusterId: string) => {
    const smbCluster: SMBCluster = {
      resource_type: CLUSTER_RESOURCE,
      cluster_id: clusterId,
      auth_mode: 'user'
    };
    return smbCluster;
  };

  it('should render cds-tabs if selection is truthy', () => {
    component.selection = selectedSmbCluster('fooBar');
    fixture.detectChanges();

    const tabsElement = fixture.debugElement.query(By.css('cds-tabs'));
    expect(tabsElement).toBeTruthy();
  });
});
