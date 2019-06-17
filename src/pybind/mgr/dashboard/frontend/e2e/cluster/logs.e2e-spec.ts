import { Helper } from '../helper.po';
import { LogsPage } from './logs.po';

describe('Logs page', () => {
  let page: LogsPage;

  beforeAll(() => {
    page = new LogsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(LogsPage.getBreadcrumbText()).toEqual('Logs');
    });

    it('should show two tabs', () => {
      expect(LogsPage.getTabsCount()).toEqual(2);
    });

    it('should show cluster logs tab at first', () => {
      expect(LogsPage.getTabText(0)).toEqual('Cluster Logs');
    });

    it('should show audit logs as a second tab', () => {
      expect(LogsPage.getTabText(1)).toEqual('Audit Logs');
    });
  });
});
