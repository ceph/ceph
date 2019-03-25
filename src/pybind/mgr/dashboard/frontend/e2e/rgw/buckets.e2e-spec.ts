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

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Buckets');
  });
});
