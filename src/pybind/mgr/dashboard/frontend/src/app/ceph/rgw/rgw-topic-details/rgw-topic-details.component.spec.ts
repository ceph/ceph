import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwTopicDetailsComponent } from './rgw-topic-details.component';
import { TopicDetails } from '~/app/shared/models/topic.model';

describe('RgwTopicDetailsComponent', () => {
  let component: RgwTopicDetailsComponent;
  let fixture: ComponentFixture<RgwTopicDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwTopicDetailsComponent]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwTopicDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });

  it('should parse policy string correctly', () => {
    const mockSelection: TopicDetails = {
      policy: '{"key": "value"}'
    };
    component.selection = mockSelection;
    component.ngOnChanges({
      selection: {
        currentValue: mockSelection,
        previousValue: null,
        firstChange: true,
        isFirstChange: true
      }
    });
    expect(component.policy).toEqual({ key: 'value' });
  });

  it('should set policy to empty object if policy is not a string', () => {
    const mockSelection: TopicDetails = {
      policy: {}
    };
    component.selection = mockSelection;
    component.ngOnChanges({
      selection: {
        currentValue: mockSelection,
        previousValue: null,
        firstChange: true,
        isFirstChange: true
      }
    });
    expect(component.policy).toEqual({});
  });
});
