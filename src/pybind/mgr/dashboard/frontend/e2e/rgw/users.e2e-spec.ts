import { Helper } from '../helper.po';
import { UsersPage } from './users.po';

describe('RGW users page', () => {
  let page: UsersPage;

  beforeAll(() => {
    page = new UsersPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Users');
  });
});
