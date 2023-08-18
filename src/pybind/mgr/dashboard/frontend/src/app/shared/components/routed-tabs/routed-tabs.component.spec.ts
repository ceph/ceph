import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RoutedTabsComponent } from './routed-tabs.component';

describe('RoutedTabsComponent', () => {
  let component: RoutedTabsComponent;
  let fixture: ComponentFixture<RoutedTabsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ RoutedTabsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RoutedTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
