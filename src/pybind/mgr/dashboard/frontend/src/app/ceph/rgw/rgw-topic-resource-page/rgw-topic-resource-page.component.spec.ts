import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of } from 'rxjs';

import { ComponentsModule } from '~/app/shared/components/components.module';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { RgwTopicResourcePageComponent } from './rgw-topic-resource-page.component';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { Topic } from '~/app/shared/models/topic.model';

describe('RgwTopicResourcePageComponent', () => {
  let component: RgwTopicResourcePageComponent;
  let fixture: ComponentFixture<RgwTopicResourcePageComponent>;
  let rgwTopicServiceSpy: { getTopic: jest.Mock };

  const mockTopic: Topic = {
    name: 'test-topic',
    owner: 'test-user',
    arn: 'arn:aws:sns:us-east-1:123456789012:test-topic',
    policy: '{"Version":"2012-10-17"}',
    subscribed_buckets: [],
    dest: {
      push_endpoint: 'http://localhost:8080',
      push_endpoint_args: '',
      push_endpoint_topic: 'test-topic',
      stored_secret: false,
      persistent: false,
      persistent_queue: 'false',
      time_to_live: 60,
      max_retries: 3,
      retry_sleep_duration: 10
    },
    key: 'test-key',
    opaqueData: ''
  };

  beforeEach(async () => {
    rgwTopicServiceSpy = {
      getTopic: jest.fn().mockReturnValue(of(mockTopic))
    };

    await TestBed.configureTestingModule({
      declarations: [RgwTopicResourcePageComponent],
      imports: [ComponentsModule, HttpClientTestingModule, PipesModule, RouterTestingModule],
      providers: [
        { provide: RgwTopicService, useValue: rgwTopicServiceSpy },
        {
          provide: ActivatedRoute,
          useValue: {
            data: of({ section: 'overview' }),
            parent: {
              paramMap: of(convertToParamMap({ name: 'test-topic' }))
            }
          }
        }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwTopicResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set section from route data', () => {
    expect(component.section).toBe('overview');
  });
});
