import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwConfigDetailsComponent } from './rgw-config-details.component';

describe('RgwConfigDetailsComponent', () => {
  let component: RgwConfigDetailsComponent;
  let fixture: ComponentFixture<RgwConfigDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwConfigDetailsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwConfigDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
