import { Helper } from '../helper.po';

describe('Hosts page', () => {
  let hosts;

  beforeAll(() => {
    hosts = new Helper().hosts;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      hosts.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(hosts.getBreadcrumbText()).toEqual('Hosts');
    });

    it('should show two tabs', () => {
      expect(hosts.getTabsCount()).toEqual(2);
    });

    it('should show hosts list tab at first', () => {
      expect(hosts.getTabText(0)).toEqual('Hosts List');
    });

    it('should show overall performance as a second tab', () => {
      expect(hosts.getTabText(1)).toEqual('Overall Performance');
    });
  });
});
