import { ComponentFixture, TestBed } from '@angular/core/testing';

import { OverviewStorageCardComponent } from './overview-storage-card.component';

describe('OverviewStorageCardComponent', () => {
  let component: OverviewStorageCardComponent;
  let fixture: ComponentFixture<OverviewStorageCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [OverviewStorageCardComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewStorageCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
