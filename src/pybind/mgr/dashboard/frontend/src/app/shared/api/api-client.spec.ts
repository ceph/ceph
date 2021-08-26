import { ApiClient } from '~/app/shared/api/api-client';

class MockApiClient extends ApiClient {}

describe('ApiClient', () => {
  const service = new MockApiClient();

  it('should get the version header value', () => {
    expect(service.getVersionHeaderValue(1, 2)).toBe('application/vnd.ceph.api.v1.2+json');
  });
});
