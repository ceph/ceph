import { Helper } from '../helper.po';
import { IscsiPage } from './iscsi.po';

describe('Iscsi Page', () => {
  let page: IscsiPage;

  beforeAll(() => {
    page = new IscsiPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Overview');
  });
});
