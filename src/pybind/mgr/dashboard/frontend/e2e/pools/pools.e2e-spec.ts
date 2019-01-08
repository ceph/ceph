import { Helper } from '../helper.po';
import { PoolsPage } from './pools.po';

describe('Pools page', () => {
  let page: PoolsPage;

  beforeAll(() => {
    page = new PoolsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Pools');
  });

  it('should show two tabs', () => {
    page.navigateTo();
    expect(Helper.getTabsCount()).toEqual(2);
  });

  it('should show pools list tab at first', () => {
    page.navigateTo();
    expect(Helper.getTabText(0)).toEqual('Pools List');
  });

  it('should show overall performance as a second tab', () => {
    page.navigateTo();
    expect(Helper.getTabText(1)).toEqual('Overall Performance');
  });
});
