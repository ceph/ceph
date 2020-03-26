import { Helper } from '../helper.po';
import { MonitorsPage } from './monitors.po';

describe('Monitors page', () => {
  let page: MonitorsPage;

  beforeAll(() => {
    page = new MonitorsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      Helper.waitTextToBePresent(Helper.getBreadcrumb(), 'Monitors');
    });
  });
});
