import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwCreateTopicFormComponent } from './rgw-create-topic-form.component';

describe('RgwCreateTopicFormComponent', () => {
  let component: RgwCreateTopicFormComponent;
  let fixture: ComponentFixture<RgwCreateTopicFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwCreateTopicFormComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwCreateTopicFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
