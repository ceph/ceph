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

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Logs');
  });

  it('should show two tabs', () => {
    page.navigateTo();
    expect(Helper.getTabsCount()).toEqual(2);
  });

  it('should show cluster logs tab at first', () => {
    page.navigateTo();
    expect(Helper.getTabText(0)).toEqual('Cluster Logs');
  });

  it('should show audit logs as a second tab', () => {
    page.navigateTo();
    expect(Helper.getTabText(1)).toEqual('Audit Logs');
  });
});
