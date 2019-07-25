import { Helper } from '../helper.po';

describe('Images page', () => {
  let images: Helper['images'];
  let pools: Helper['pools'];

  beforeAll(() => {
    images = new Helper().images;
    pools = new Helper().pools;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(() => {
      images.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(images.getBreadcrumbText()).toEqual('Images');
    });

    it('should show three tabs', () => {
      expect(images.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', () => {
      expect(images.getTabText(0)).toEqual('Images');
      expect(images.getTabText(1)).toEqual('Trash');
      expect(images.getTabText(2)).toEqual('Overall Performance');
    });
  });

  describe('create, edit & delete image test', () => {
    const poolName = 'e2e_images_pool';
    const imageName = 'e2e_images_image';
    const newImageName = 'e2e_images_image_new';

    beforeAll(() => {
      pools.navigateTo('create'); // Need pool for image testing
      pools.create(poolName, 8, 'rbd').then(() => {
        pools.navigateTo();
        pools.exist(poolName, true);
      });
      images.navigateTo();
    });

    it('should create image', () => {
      images.createImage(imageName, poolName, '1');
      expect(images.getTableCell(imageName).isPresent()).toBe(true);
    });

    it('should edit image', () => {
      images.editImage(imageName, poolName, newImageName, '2');
      expect(images.getTable().getText()).toMatch(newImageName);
    });

    it('should delete image', () => {
      images.deleteImage(newImageName);
      expect(images.getTableCell(newImageName).isPresent()).toBe(false);
    });

    afterAll(() => {
      pools.navigateTo(); // Deletes images test pool
      pools.delete(poolName).then(() => {
        pools.navigateTo();
        pools.exist(poolName, false);
      });
    });
  });
});
