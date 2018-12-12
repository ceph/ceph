import { Helper } from '../helper.po';
import { MirroringPage } from './mirroring.po';

describe('Mirroring page', () => {
  let page: MirroringPage;

  beforeAll(() => {
    page = new MirroringPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Mirroring');
  });
});
