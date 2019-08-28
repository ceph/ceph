import { Helper } from '../helper.po';
import { PoolPageHelper } from '../pools/pools.po';
import { ImagesPageHelper } from './images.po';

describe('Images page', () => {
  let pools: PoolPageHelper;
  let images: ImagesPageHelper;

  beforeAll(() => {
    images = new ImagesPageHelper();
    pools = new PoolPageHelper();
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await images.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await expect(images.getBreadcrumbText()).toEqual('Images');
    });

    it('should show three tabs', async () => {
      await expect(images.getTabsCount()).toEqual(3);
    });

    it('should show text for all tabs', async () => {
      await expect(images.getTabText(0)).toEqual('Images');
      await expect(images.getTabText(1)).toEqual('Trash');
      await expect(images.getTabText(2)).toEqual('Overall Performance');
    });
  });

  describe('create, edit & delete image test', async () => {
    const poolName = 'e2e_images_pool';
    const imageName = 'e2e_images_image';
    const newImageName = 'e2e_images_image_new';

    beforeAll(async () => {
      await pools.navigateTo('create'); // Need pool for image testing
      await pools.create(poolName, 8, 'rbd');
      await pools.navigateTo();
      await pools.exist(poolName, true);
      await images.navigateTo();
    });

    it('should create image', async () => {
      await images.createImage(imageName, poolName, '1');
      await expect(images.getTableCell(imageName).isPresent()).toBe(true);
    });

    it('should edit image', async () => {
      await images.editImage(imageName, poolName, newImageName, '2');
      await expect(images.getTableCell(newImageName).isPresent()).toBe(true);
    });

    it('should delete image', async () => {
      await images.deleteImage(newImageName);
      await expect(images.getTableCell(newImageName).isPresent()).toBe(false);
    });

    afterAll(async () => {
      await pools.navigateTo(); // Deletes images test pool
      await pools.delete(poolName);
      await pools.navigateTo();
      await pools.exist(poolName, false);
    });
  });
});
