import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwUserTabsComponent } from './rgw-user-tabs.component';

describe('RgwUserTabsComponent', () => {
  let component: RgwUserTabsComponent;
  let fixture: ComponentFixture<RgwUserTabsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwUserTabsComponent]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
