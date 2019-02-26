import { Helper } from '../helper.po';
import { OSDsPage } from './osds.po';

describe('OSDs page', () => {
  let page: OSDsPage;

  beforeAll(() => {
    page = new OSDsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('OSDs');
  });

  it('should show two tabs', () => {
    page.navigateTo();
    expect(Helper.getTabsCount()).toEqual(2);
  });

  it('should show OSDs list tab at first', () => {
    page.navigateTo();
    expect(Helper.getTabText(0)).toEqual('OSDs List');
  });

  it('should show overall performance as a second tab', () => {
    page.navigateTo();
    expect(Helper.getTabText(1)).toEqual('Overall Performance');
  });
});
