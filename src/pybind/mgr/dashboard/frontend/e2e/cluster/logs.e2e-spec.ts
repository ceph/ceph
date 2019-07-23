import { Helper } from '../helper.po';

describe('Logs page', () => {
  let logs;

  beforeAll(() => {
    logs = new Helper().logs;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      logs.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(logs.getBreadcrumbText()).toEqual('Logs');
    });

    it('should show two tabs', () => {
      expect(logs.getTabsCount()).toEqual(2);
    });

    it('should show cluster logs tab at first', () => {
      expect(logs.getTabText(0)).toEqual('Cluster Logs');
    });

    it('should show audit logs as a second tab', () => {
      expect(logs.getTabText(1)).toEqual('Audit Logs');
    });
  });
});
