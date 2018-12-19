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
});
