import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { of } from 'rxjs';

import { RgwTopicResourceSidebarComponent } from './rgw-topic-resource-sidebar.component';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
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

describe('RgwTopicResourceSidebarComponent', () => {
  let component: RgwTopicResourceSidebarComponent;
  let fixture: ComponentFixture<RgwTopicResourceSidebarComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwTopicResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ name: 'topic:ownerName:testHttp' }))
          }
        },
        {
          provide: RgwTopicService,
          useValue: {
            getTopic: () =>
              of({
                name: 'testHttp',
                owner: 'ownerName',
                arn: 'arnValue',
                dest: mockDestination,
                policy: '{}',
                key: 'topic:ownerName:testHttp',
                opaqueData: 'test@12345',
                subscribed_buckets: []
              } as Topic)
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwTopicResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set topic title and sidebar items', () => {
    expect(component.topicName).toBe('testHttp');
    expect(component.sidebarItems.length).toBe(3);
    expect(component.sidebarItems[0].label).toBe('Overview');
  });
});
