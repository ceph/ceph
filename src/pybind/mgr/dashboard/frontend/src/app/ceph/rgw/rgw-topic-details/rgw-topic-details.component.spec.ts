import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwTopicDetailsComponent } from './rgw-topic-details.component';

describe('RgwTopicDetailsComponent', () => {
  let component: RgwTopicDetailsComponent;
  let fixture: ComponentFixture<RgwTopicDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwTopicDetailsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwTopicDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
