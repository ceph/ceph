import { Helper } from '../helper.po';
import { HostsPage } from './hosts.po';

describe('Hosts page', () => {
  let page: HostsPage;

  beforeAll(() => {
    page = new HostsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Hosts');
  });

  it('should show two tabs', () => {
    page.navigateTo();
    expect(Helper.getTabsCount()).toEqual(2);
  });

  it('should show hosts list tab at first', () => {
    page.navigateTo();
    expect(Helper.getTabText(0)).toEqual('Hosts List');
  });

  it('should show overall performance as a second tab', () => {
    page.navigateTo();
    expect(Helper.getTabText(1)).toEqual('Overall Performance');
  });
});
