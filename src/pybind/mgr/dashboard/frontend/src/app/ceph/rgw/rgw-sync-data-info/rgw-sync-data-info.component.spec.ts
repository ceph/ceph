import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwSyncDataInfoComponent } from './rgw-sync-data-info.component';
import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwSyncDataInfoComponent', () => {
  let component: RgwSyncDataInfoComponent;
  let fixture: ComponentFixture<RgwSyncDataInfoComponent>;

  configureTestBed({
    declarations: [RgwSyncDataInfoComponent],
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
});
