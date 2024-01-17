import { ComponentFixture, TestBed, fakeAsync } from '@angular/core/testing';

import { RgwSyncDataInfoComponent } from './rgw-sync-data-info.component';
import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';
import { By } from '@angular/platform-browser';

describe('RgwSyncDataInfoComponent', () => {
  let component: RgwSyncDataInfoComponent;
  let fixture: ComponentFixture<RgwSyncDataInfoComponent>;

  configureTestBed({
    declarations: [RgwSyncDataInfoComponent, RelativeDatePipe],
    imports: [NgbPopoverModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwSyncDataInfoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display "Up to Date" badge when zone is up to date', () => {
    component.zone = {
      timestamp: null
    };
    fixture.detectChanges();
    const upToDateBadge = fixture.debugElement.query(By.css('.badge-success'));
    expect(upToDateBadge).toBeTruthy();
    expect(upToDateBadge.nativeElement.textContent).toEqual('Up to Date');
  });

  it('should display correct sync status and last synced time', fakeAsync(() => {
    component.zone = { syncstatus: 'Syncing', timestamp: new Date(Date.now() - 10 * 60 * 1000) };
    fixture.detectChanges();

    const statusElement = fixture.debugElement.query(By.css('li b'));
    expect(statusElement.nativeElement.textContent).toContain('Status:');

    const lastSyncedElement = fixture.debugElement.query(By.css('li.mt-4.fw-bold'));
    expect(lastSyncedElement.nativeElement.textContent).toContain('Last Synced:');
    const lastSyncedTimestamp = fixture.debugElement.query(By.css('.badge-info'));
    expect(lastSyncedTimestamp.nativeElement.textContent).toEqual('10 minutes ago');
  }));

  it('should display sync status in the popover', () => {
    component.zone = {
      syncstatus: 'Syncing',
      timestamp: new Date(Date.now() - 10 * 60 * 1000),
      fullSyncStatus: [
        'full sync: 0/128 shards',
        'incremental sync:128/128 shards',
        'Data is behind on 31 shards'
      ]
    };
    fixture.detectChanges();
    const syncStatus = fixture.debugElement.query(By.css('.text-primary'));
    expect(syncStatus).toBeTruthy();
    expect(syncStatus.nativeElement.textContent).toEqual('Syncing');
    const syncPopover = fixture.debugElement.query(By.css('a'));
    syncPopover.triggerEventHandler('click', null);
    fixture.detectChanges();
    expect(syncPopover).toBeTruthy();
    const syncPopoverText = fixture.debugElement.query(By.css('.text-center'));
    expect(syncPopoverText.nativeElement.textContent).toEqual(
      'Sync Status:Full Sync:0/128 Shards Incremental Sync:128/128 Shards: Data Is Behind On 31 Shards'
    );
  });
});
