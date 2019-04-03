import { Helper } from '../helper.po';
import { DaemonsPage } from './daemons.po';

describe('RGW daemons page', () => {
  let page: DaemonsPage;

  beforeAll(() => {
    page = new DaemonsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Daemons');
  });
});
