import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwCreateNotificationFormComponent } from './rgw-create-notification-form.component';

describe('RgwCreateNotificationFormComponent', () => {
  let component: RgwCreateNotificationFormComponent;
  let fixture: ComponentFixture<RgwCreateNotificationFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RgwCreateNotificationFormComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(RgwCreateNotificationFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
