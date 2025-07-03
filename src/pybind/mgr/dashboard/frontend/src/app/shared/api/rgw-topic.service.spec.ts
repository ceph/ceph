import { TestBed } from '@angular/core/testing';
import { RgwTopicService } from './rgw-topic.service';
import { configureTestBed, RgwHelper } from '~/testing/unit-test-helper';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

describe('RgwTopicService', () => {
  let service: RgwTopicService;
  let httpTesting: HttpTestingController;
  configureTestBed({
    imports: [HttpClientTestingModule]
  });
  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [RgwTopicService]
  });

  beforeEach(() => {
    service = TestBed.inject(RgwTopicService);
    httpTesting = TestBed.inject(HttpTestingController);
    RgwHelper.selectDaemon();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
  it('should call list with result', () => {
    service.listTopic().subscribe((resp) => {
      let result = resp;
      return result;
    });
    let req = httpTesting.expectOne(`api/rgw/topic`);
    expect(req.request.method).toBe('GET');
    req.flush(['foo', 'bar']);
  });

  it('should call create', () => {
    service.create({} as any).subscribe();
    const req = httpTesting.expectOne(`api/rgw/topic?${RgwHelper.DAEMON_QUERY_PARAM}`);
    expect(req.request.method).toBe('POST');
  });
  it('should call update', () => {
    service.create({} as any).subscribe();
    const req = httpTesting.expectOne(`api/rgw/topic?${RgwHelper.DAEMON_QUERY_PARAM}`);
    expect(req.request.method).toBe('POST');
  });
  it('should call delete', () => {
    service.delete('foo').subscribe();
    const req = httpTesting.expectOne(`api/rgw/topic/foo`);
    expect(req.request.method).toBe('DELETE');
  });
});
