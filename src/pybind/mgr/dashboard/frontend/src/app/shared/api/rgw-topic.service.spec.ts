import { TestBed } from '@angular/core/testing';

import { RgwTopicService } from './rgw-topic.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { TopicModel } from '~/app/ceph/rgw/rgw-topic-list/topic.model';

const mockTopicData: TopicModel[] = [
  {
    owner: 'dashboard',
    name: 'httpTopic',
    dest: {
      push_endpoint: 'https://10.0.66.13:443',
      push_endpoint_args: 'use_ssl=false&verify_ssl=true',
      push_endpoint_topic: 'httpTopic',
      stored_secret: false,
      persistent: true,
      persistent_queue: ':httpTopic',
      time_to_live: '2',
      max_retries: '4',
      retry_sleep_duration: '2'
    },
    arn: 'arn:aws:sns:zg1-realm1::httpTopic',
    opaqueData: 'test@1236',
    policy:
      '{\n  "Statement": [\n    {\n      "Sid": "grant-1234-publish",\n      "Effect": "Allow",\n      "Principal": {\n        "AWS": "111122223333"\n      },\n      "Action": [\n        "sns:Publish"\n      ],\n      "Resource": "arn:aws:sns:us-east-2:444455556666:MyTopic"\n    }\n  ]\n}',
    subscribed_buckets: []
  },
  {
    owner: 'dashboard',
    name: 'httpTopic1',
    dest: {
      push_endpoint: 'https://10.0.66.13:443',
      push_endpoint_args: 'use_ssl=false&verify_ssl=true',
      push_endpoint_topic: 'httpTopic1',
      stored_secret: false,
      persistent: true,
      persistent_queue: ':httpTopic1',
      time_to_live: '2',
      max_retries: '4',
      retry_sleep_duration: '2'
    },
    arn: 'arn:aws:sns:zg1-realm1::httpTopic1',
    opaqueData: 'test123',
    policy:
      '{\n  "Statement": [\n    {\n      "Sid": "grant-1234-publish",\n      "Effect": "Allow",\n      "Principal": {\n        "AWS": "111122223333"\n      },\n      "Action": [\n        "sns:Publish"\n      ],\n      "Resource": "arn:aws:sns:us-east-2:444455556666:MyTopic"\n    }\n  ]\n}',
    subscribed_buckets: []
  }
];
describe('RgwTopicService', () => {
  let service: RgwTopicService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [RgwTopicService]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(RgwTopicService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
  it('should fetch detailed list of accounts', () => {
    service.listTopic().subscribe();
    const req = httpTesting.expectOne('api/rgw/topic');
    expect(req.request.method).toBe('GET');
    req.flush(mockTopicData);
  });
});
