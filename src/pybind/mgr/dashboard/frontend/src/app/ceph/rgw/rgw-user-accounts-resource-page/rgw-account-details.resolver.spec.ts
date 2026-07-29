import { configureTestBed } from '~/testing/unit-test-helper';
import { TestBed } from '@angular/core/testing';
import { convertToParamMap } from '@angular/router';
import { of } from 'rxjs';

import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';
import { RgwAccountDetailsResolver } from './rgw-account-details.resolver';

describe('RgwAccountDetailsResolver', () => {
  let resolver: RgwAccountDetailsResolver;

  configureTestBed({
    providers: [
      RgwAccountDetailsResolver,
      {
        provide: RgwUserAccountsService,
        useValue: {
          list: () =>
            of([
              {
                id: 'RGW11111111111111111',
                name: 'account1'
              },
              {
                id: 'RGW22222222222222222',
                name: 'account2'
              }
            ])
        }
      }
    ]
  });

  beforeEach(() => {
    resolver = TestBed.inject(RgwAccountDetailsResolver);
  });

  it('should resolve account by account name', (done) => {
    const route: any = {
      paramMap: convertToParamMap({ accountName: 'account2' })
    };

    resolver.resolve(route).subscribe((account) => {
      if (account?.id !== 'RGW22222222222222222') {
        done(new Error('Failed to resolve account by account name'));
        return;
      }

      done();
    });
  });
});
