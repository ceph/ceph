import { Helper } from '../helper.po';
import { CrushMapPage } from './crush-map.po';

describe('CRUSH map page', () => {
  let page: CrushMapPage;

  beforeAll(() => {
    page = new CrushMapPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('CRUSH map');
  });
});
