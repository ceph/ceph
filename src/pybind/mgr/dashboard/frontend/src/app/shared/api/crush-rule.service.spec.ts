import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { CrushRuleService } from './crush-rule.service';

describe('CrushRuleService', () => {
  let service: CrushRuleService;
  let httpTesting: HttpTestingController;
  const apiPath = 'api/crush_rule';

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [CrushRuleService]
  });

  beforeEach(() => {
    service = TestBed.inject(CrushRuleService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call create', () => {
    service.create({ root: 'default', name: 'someRule', failure_domain: 'osd' }).subscribe();
    const req = httpTesting.expectOne(apiPath);
    expect(req.request.method).toBe('POST');
  });

  it('should call delete', () => {
    service.delete('test').subscribe();
    const req = httpTesting.expectOne(`${apiPath}/test`);
    expect(req.request.method).toBe('DELETE');
  });

  it('should call getInfo', () => {
    service.getInfo().subscribe();
    const req = httpTesting.expectOne(`ui-${apiPath}/info`);
    expect(req.request.method).toBe('GET');
  });
});
