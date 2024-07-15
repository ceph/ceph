import { PoolPageHelper } from '../pools/pools.po';
import { ImagesPageHelper } from './images.po';

describe('Images page', () => {
  const pools = new PoolPageHelper();
  const images = new ImagesPageHelper();

  const poolName = 'e2e_images_pool';

  before(() => {
    cy.login();
    // Need pool for image testing
    pools.navigateTo('create');
    pools.create(poolName, 8, ['rbd']);
    pools.existTableCell(poolName);
  });

  after(() => {
    // Deletes images test pool
    pools.navigateTo();
    pools.delete(poolName);
    pools.navigateTo();
    pools.existTableCell(poolName, false);
  });

  beforeEach(() => {
    cy.login();
    images.navigateTo();
  });

  it('should open and show breadcrumb', () => {
    images.expectBreadcrumbText('Images');
  });

  it('should show four tabs', () => {
    images.getTabsCount().should('eq', 4);
  });

  it('should show text for all tabs', () => {
    images.getTabText(0).should('eq', 'Images');
    images.getTabText(1).should('eq', 'Namespaces');
    images.getTabText(2).should('eq', 'Trash');
    images.getTabText(3).should('eq', 'Overall Performance');
  });

  describe('create, edit & delete image test', () => {
    const imageName = 'e2e_images#image';
    const newImageName = 'e2e_images#image_new';

    it('should create image', () => {
      images.createImage(imageName, poolName, '1');
      images.getFirstTableCell(imageName).should('exist');
    });

    it('should edit image', () => {
      images.editImage(imageName, poolName, newImageName, '2');
      images.getFirstTableCell(newImageName).should('exist');
    });

    it('should delete image', () => {
      images.delete(newImageName);
    });
  });

  describe('move to trash, restore and purge image tests', () => {
    const imageName = 'e2e_trash#image';
    const newImageName = 'e2e_newtrash#image';

    before(() => {
      cy.login();
      // Need image for trash testing
      images.createImage(imageName, poolName, '1');
      images.getFirstTableCell(imageName).should('exist');
    });

    it('should move the image to the trash', () => {
      images.moveToTrash(imageName);
      images.getFirstTableCell(imageName).should('exist');
    });

    it('should restore image to images table', () => {
      images.restoreImage(imageName, newImageName);
      images.getFirstTableCell(newImageName).should('exist');
    });

    it('should purge trash in images trash tab', () => {
      images.getFirstTableCell(newImageName).should('exist');
      images.moveToTrash(newImageName);
      images.purgeTrash(newImageName, poolName);
    });
  });
});
