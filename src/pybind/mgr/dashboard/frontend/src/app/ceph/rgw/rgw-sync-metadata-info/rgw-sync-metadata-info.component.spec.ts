import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwSyncMetadataInfoComponent } from './rgw-sync-metadata-info.component';
import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwSyncMetadataInfoComponent', () => {
  let component: RgwSyncMetadataInfoComponent;
  let fixture: ComponentFixture<RgwSyncMetadataInfoComponent>;

  configureTestBed({
    declarations: [RgwSyncMetadataInfoComponent],
    imports: [NgbPopoverModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwSyncMetadataInfoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
