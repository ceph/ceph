import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { IscsiTargetDetailsComponent } from './iscsi-target-details.component';

describe('IscsiTargetDetailsComponent', () => {
  let component: IscsiTargetDetailsComponent;
  let fixture: ComponentFixture<IscsiTargetDetailsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ IscsiTargetDetailsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
