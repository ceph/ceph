import { convertToParamMap } from '@angular/router';
import { expect } from '@jest/globals';

import { RgwAccountDetailsBreadcrumbResolver } from './rgw-account-details-breadcrumb.resolver';

describe('RgwAccountDetailsBreadcrumbResolver', () => {
  it('should include account name in breadcrumb', () => {
    const resolver = new RgwAccountDetailsBreadcrumbResolver();
    const route: any = {
      url: [{ path: 'account1' }],
      pathFromRoot: [
        { url: [] },
        { url: [{ path: 'rgw' }] },
        { url: [{ path: 'accounts' }] },
        { url: [{ path: 'account1' }] }
      ],
      paramMap: convertToParamMap({ accountName: 'account1' }),
      data: {
        account: {
          name: 'account1'
        }
      }
    };

    const result = resolver.resolve(route);

    expect(result.length).toBe(1);
    expect(result[0].text).toBe('account1');
    expect(result[0].path).toBe('/rgw/accounts/account1');
  });
});
