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

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(HostsPage.getBreadcrumbText()).toEqual('Hosts');
    });

    it('should show two tabs', () => {
      expect(HostsPage.getTabsCount()).toEqual(2);
    });

    it('should show hosts list tab at first', () => {
      expect(HostsPage.getTabText(0)).toEqual('Hosts List');
    });

    it('should show overall performance as a second tab', () => {
      expect(HostsPage.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
