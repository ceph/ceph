import { Helper } from '../helper.po';
import { ImagesPage } from './images.po';

describe('Images page', () => {
  let page: ImagesPage;

  beforeAll(() => {
    page = new ImagesPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Images');
  });
});
