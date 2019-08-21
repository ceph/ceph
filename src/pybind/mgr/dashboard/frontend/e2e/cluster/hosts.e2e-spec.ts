import { Helper } from '../helper.po';
import { HostsPageHelper } from './hosts.po';

describe('Hosts page', () => {
  let hosts: HostsPageHelper;

  beforeAll(() => {
    hosts = new Helper().hosts;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await hosts.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await hosts.getBreadcrumbText()).toEqual('Hosts');
    });

    it('should show two tabs', async () => {
      expect(await hosts.getTabsCount()).toEqual(2);
    });

    it('should show hosts list tab at first', async () => {
      expect(await hosts.getTabText(0)).toEqual('Hosts List');
    });

    it('should show overall performance as a second tab', async () => {
      expect(await hosts.getTabText(1)).toEqual('Overall Performance');
    });
  });

  describe('services link test', () => {
    it('should check at least one host is present', async () => {
      await hosts.check_for_host();
    });

    it('should check services link(s) work for first host', async () => {
      await hosts.check_services_links();
    });
  });
});
