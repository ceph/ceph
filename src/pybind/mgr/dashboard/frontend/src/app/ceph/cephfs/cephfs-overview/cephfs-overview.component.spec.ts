import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsOverviewComponent } from './cephfs-overview.component';

describe('CephfsOverviewComponent', () => {
  let component: CephfsOverviewComponent;
  let fixture: ComponentFixture<CephfsOverviewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CephfsOverviewComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CephfsOverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
