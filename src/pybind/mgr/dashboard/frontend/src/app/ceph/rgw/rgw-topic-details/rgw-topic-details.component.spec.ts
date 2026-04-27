import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwTopicDetailsComponent } from './rgw-topic-details.component';
import { Topic } from '~/app/shared/models/topic.model';

interface Destination {
  push_endpoint: string;
  push_endpoint_args: string;
  push_endpoint_topic: string;
  stored_secret: boolean;
  persistent: boolean;
  persistent_queue: string;
  time_to_live: number;
  max_retries: number;
  retry_sleep_duration: number;
}

const mockDestination: Destination = {
  push_endpoint: 'http://localhost:8080',
  push_endpoint_args: 'args',
  push_endpoint_topic: 'topic',
  stored_secret: false,
  persistent: true,
  persistent_queue: 'queue',
  time_to_live: 3600,
  max_retries: 5,
  retry_sleep_duration: 10
};

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

  it('should parse policy string correctly', () => {
    const mockSelection: Topic = {
      name: 'testHttp',
      owner: 'ownerName',
      arn: 'arnValue',
      dest: mockDestination,
      policy: '{"key": "value"}',
      key: 'topic:ownerName:testHttp',
      opaqueData: 'test@12345',
      subscribed_buckets: []
    };

    component.selection = mockSelection;
    component.ngOnChanges({
      selection: {
        currentValue: mockSelection,
        previousValue: null,
        firstChange: true,
        isFirstChange: () => true
      }
    });

    expect(component.policy).toEqual({ key: 'value' });
  });

  it('should set policy to empty object if policy is not a string', () => {
    const mockSelection: Topic = {
      name: 'testHttp',
      owner: 'ownerName',
      arn: 'arnValue',
      dest: mockDestination,
      policy: '{}',
      key: 'topic:ownerName:testHttp',
      subscribed_buckets: [],
      opaqueData: ''
    };

    component.selection = mockSelection;
    component.ngOnChanges({
      selection: {
        currentValue: mockSelection,
        previousValue: null,
        firstChange: true,
        isFirstChange: () => true
      }
    });

    expect(component.policy).toEqual({});
  });
});
