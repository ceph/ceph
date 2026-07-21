import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BlockOverviewComponent } from './block-overview.component';

describe('BlockOverviewComponent', () => {
  let component: BlockOverviewComponent;
  let fixture: ComponentFixture<BlockOverviewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [BlockOverviewComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(BlockOverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
