import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwSyncMetadataInfoComponent } from './rgw-sync-metadata-info.component';
import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';

describe('RgwSyncMetadataInfoComponent', () => {
  let component: RgwSyncMetadataInfoComponent;
  let fixture: ComponentFixture<RgwSyncMetadataInfoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwSyncMetadataInfoComponent],
      imports: [NgbPopoverModule]
    }).compileComponents();
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
