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

  describe('breadcrumb test', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      Helper.waitTextToBePresent(Helper.getBreadcrumb(), 'Overview');
    });
  });
});
