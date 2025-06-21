import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwTopicDetailsComponent } from './rgw-topic-details.component';
import { Topic, Destination } from '~/app/shared/models/topic.model';

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
      dest: {} as Destination,
      policy: '{"key": "value"}',
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
      dest: {} as Destination,
      policy: '{}',
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
