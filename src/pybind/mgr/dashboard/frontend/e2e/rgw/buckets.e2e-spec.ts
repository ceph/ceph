import { Helper } from '../helper.po';
import { BucketsPage } from './buckets.po';

describe('RGW buckets page', () => {
  let page: BucketsPage;

  beforeAll(() => {
    page = new BucketsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(Helper.getBreadcrumbText()).toEqual('Buckets');
    });
  });
});
