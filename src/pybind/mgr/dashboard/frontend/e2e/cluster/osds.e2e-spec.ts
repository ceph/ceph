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

  it('should open and show breadcrumnb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('OSDs');
  });
});
