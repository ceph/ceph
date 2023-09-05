import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwSyncDataInfoComponent } from './rgw-sync-data-info.component';
import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';

describe('RgwSyncDataInfoComponent', () => {
  let component: RgwSyncDataInfoComponent;
  let fixture: ComponentFixture<RgwSyncDataInfoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwSyncDataInfoComponent],
      imports: [NgbPopoverModule]
    }).compileComponents();
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
