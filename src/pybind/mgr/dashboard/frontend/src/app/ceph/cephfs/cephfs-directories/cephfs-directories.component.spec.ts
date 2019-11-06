import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NodeEvent, Tree, TreeModel, TreeModule } from 'ng2-tree';
import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { CephfsService } from '../../../shared/api/cephfs.service';
import {
  CephfsDir,
  CephfsQuotas,
  CephfsSnapshot
} from '../../../shared/models/cephfs-directory-models';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsDirectoriesComponent } from './cephfs-directories.component';

describe('CephfsDirectoriesComponent', () => {
  let component: CephfsDirectoriesComponent;
  let fixture: ComponentFixture<CephfsDirectoriesComponent>;
  let lsDirSpy;
  let originalDate;

  // Get's private attributes or functions
  const get = {
    nodeIds: (): { [path: string]: CephfsDir } => component['nodeIds'],
    dirs: (): CephfsDir[] => component['dirs'],
    requestedPaths: (): string[] => component['requestedPaths']
  };

  // Object contains mock data that will be reset before each test.
  let mockData: {
    nodes: TreeModel[];
    parent: Tree;
  };

  // Object contains mock functions
  const mockLib = {
    quotas: (max_bytes: number, max_files: number): CephfsQuotas => ({ max_bytes, max_files }),
    snapshots: (dirPath: string, howMany: number): CephfsSnapshot[] => {
      const name = 'someSnapshot';
      const snapshots = [];
      for (let i = 0; i < howMany; i++) {
        const path = `${dirPath}/.snap/${name}${i}`;
        const created = new Date(
          +new Date() - 3600 * 24 * 1000 * howMany * (howMany - i)
        ).toString();
        snapshots.push({ name, path, created });
      }
      return snapshots;
    },
    dir: (path: string, name: string, modifier: number): CephfsDir => {
      const dirPath = `${path === '/' ? '' : path}/${name}`;
      return {
        name,
        path: dirPath,
        parent: path,
        quotas: mockLib.quotas(1024 * modifier, 10 * modifier),
        snapshots: mockLib.snapshots(path, modifier)
      };
    },
    // Only used inside other mocks
    lsSingleDir: (path = ''): CephfsDir[] => {
      if (path.includes('b')) {
        // 'b' has no sub directories
        return [];
      }
      return [
        // Directories are not sorted!
        mockLib.dir(path, 'c', 3),
        mockLib.dir(path, 'a', 1),
        mockLib.dir(path, 'b', 2)
      ];
    },
    lsDir: (_id: number, path = '') => {
      // will return 2 levels deep
      let data = mockLib.lsSingleDir(path);
      const paths = data.map((dir) => dir.path);
      paths.forEach((pathL2) => {
        data = data.concat(mockLib.lsSingleDir(pathL2));
      });
      return of(data);
    },
    date: (arg) => (arg ? new originalDate(arg) : new Date('2022-02-22T00:00:00')),
    getControllerByPath: (path: string) => {
      return {
        expand: () => mockLib.expand(path),
        select: () => component.onNodeSelected(mockLib.getNodeEvent(path))
      };
    },
    // Only used inside other mocks to mock "tree.expand" of every node
    expand: (path: string) => {
      component.updateDirectory(path, (nodes) => (mockData.nodes = mockData.nodes.concat(nodes)));
    },
    getNodeEvent: (path: string): NodeEvent => {
      const tree = mockData.nodes.find((n) => n.id === path) as Tree;
      if (mockData.parent) {
        tree.parent = mockData.parent;
      } else {
        const dir = get.nodeIds()[path];
        const parentNode = mockData.nodes.find((n) => n.id === dir.parent);
        tree.parent = parentNode as Tree;
      }
      return { node: tree } as NodeEvent;
    },
    changeId: (id: number) => {
      component.id = id;
      component.ngOnChanges();
      mockData.nodes = [component.tree].concat(component.tree.children);
    },
    selectNode: (path: string) => {
      mockLib.getControllerByPath(path).select();
    },
    mkDir: (path: string, name: string, maxFiles: number, maxBytes: number) => {
      const dir = mockLib.dir(path, name, 3);
      dir.quotas.max_bytes = maxBytes * 1024;
      dir.quotas.max_files = maxFiles;
      get.nodeIds()[dir.path] = dir;
      mockData.nodes.push({
        id: dir.path,
        value: name
      });
    }
  };

  // Expects that are used frequently
  const assert = {
    dirLength: (n: number) => expect(get.dirs().length).toBe(n),
    nodeLength: (n: number) => expect(mockData.nodes.length).toBe(n),
    lsDirCalledTimes: (n: number) => expect(lsDirSpy).toHaveBeenCalledTimes(n),
    requestedPaths: (expected: string[]) => expect(get.requestedPaths()).toEqual(expected),
    quotaSettings: (
      fileValue: number | string,
      fileOrigin: string,
      sizeValue: string,
      sizeOrigin: string
    ) =>
      expect(component.settings).toEqual([
        { name: 'Max files', value: fileValue, origin: fileOrigin },
        { name: 'Max size', value: sizeValue, origin: sizeOrigin }
      ])
  };

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, TreeModule],
    declarations: [CephfsDirectoriesComponent],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    mockData = {
      nodes: undefined,
      parent: undefined
    };
    originalDate = Date;
    spyOn(global, 'Date').and.callFake(mockLib.date);

    lsDirSpy = spyOn(TestBed.get(CephfsService), 'lsDir').and.callFake(mockLib.lsDir);

    fixture = TestBed.createComponent(CephfsDirectoriesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    spyOn(component.treeComponent, 'getControllerByNodeId').and.callFake((id) =>
      mockLib.getControllerByPath(id)
    );
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('calls lsDir only if an id exits', () => {
    component.ngOnChanges();
    assert.lsDirCalledTimes(0);

    mockLib.changeId(1);
    assert.lsDirCalledTimes(1);
    expect(lsDirSpy).toHaveBeenCalledWith(1, '/');

    mockLib.changeId(2);
    assert.lsDirCalledTimes(2);
    expect(lsDirSpy).toHaveBeenCalledWith(2, '/');
  });

  describe('listing sub directories', () => {
    beforeEach(() => {
      mockLib.changeId(1);
      /**
       * Tree looks like this:
       * v /
       *   > a
       *   * b
       *   > c
       * */
    });

    it('expands first level', () => {
      // Tree will only show '*' if nor 'loadChildren' or 'children' are defined
      expect(
        mockData.nodes.map((node) => ({ [node.id]: Boolean(node.loadChildren || node.children) }))
      ).toEqual([{ '/': true }, { '/a': true }, { '/b': false }, { '/c': true }]);
    });

    it('resets all dynamic content on id change', () => {
      mockLib.selectNode('/a');
      /**
       * Tree looks like this:
       * v /
       *   v a <- Selected
       *     > a
       *     * b
       *     > c
       *   * b
       *   > c
       * */
      assert.requestedPaths(['/', '/a']);
      assert.nodeLength(7);
      assert.dirLength(15);
      expect(component.selectedDir).toBeDefined();

      mockLib.changeId(undefined);
      assert.dirLength(0);
      assert.requestedPaths([]);
      expect(component.selectedDir).not.toBeDefined();
    });

    it('should select a node and show the directory contents', () => {
      mockLib.selectNode('/a');
      const dir = get.dirs().find((d) => d.path === '/a');
      expect(component.selectedDir).toEqual(dir);
      assert.quotaSettings(10, '/a', '1 KiB', '/a');
    });

    it('should extend the list by subdirectories when expanding and omit already called path', () => {
      mockLib.selectNode('/a');
      mockLib.selectNode('/a/c');
      /**
       * Tree looks like this:
       * v /
       *   v a
       *     > a
       *     * b
       *     v c <- Selected
       *       > a
       *       * b
       *       > c
       *   * b
       *   > c
       * */
      assert.lsDirCalledTimes(3);
      assert.requestedPaths(['/', '/a', '/a/c']);
      assert.dirLength(21);
      assert.nodeLength(10);
    });

    it('should select parent by path', () => {
      mockLib.selectNode('/a');
      mockLib.selectNode('/a/c');
      mockLib.selectNode('/a/c/a');
      component.selectOrigin('/a');
      expect(component.selectedDir.path).toBe('/a');
    });

    it('should omit call for directories that have no sub directories', () => {
      mockLib.selectNode('/b');
      /**
       * Tree looks like this:
       * v /
       *   > a
       *   * b <- Selected
       *   > c
       * */
      assert.lsDirCalledTimes(1);
      assert.requestedPaths(['/']);
      assert.nodeLength(4);
    });

    describe('used quotas', () => {
      const setUpDirs = (quotas: number[][]) => {
        let path = '';
        quotas.forEach((quota, index) => {
          index += 1;
          mockLib.mkDir(path, index.toString(), quota[0], quota[1]);
          path += '/' + index;
        });
        mockData.parent = {
          value: '3',
          id: '/1/2/3',
          parent: {
            value: '2',
            id: '/1/2',
            parent: {
              value: '1',
              id: '/1',
              parent: { value: '/', id: '/' }
            }
          }
        } as Tree;
        mockLib.selectNode('/1/2/3/4');
      };

      it('should use no quota if none is set', () => {
        setUpDirs([[0, 0], [0, 0], [0, 0], [0, 0]]);
        assert.quotaSettings('', '', '', '');
      });

      it('should use quota from upper parents', () => {
        setUpDirs([[100, 0], [0, 8], [0, 0], [0, 0]]);
        assert.quotaSettings(100, '/1', '8 KiB', '/1/2');
      });

      it('should use quota from the parent with the lowest value (deep inheritance)', () => {
        setUpDirs([[200, 1], [100, 4], [400, 3], [300, 2]]);
        assert.quotaSettings(100, '/1/2', '1 KiB', '/1');
      });

      it('should use current value', () => {
        setUpDirs([[200, 2], [300, 4], [400, 3], [100, 1]]);
        assert.quotaSettings(100, '/1/2/3/4', '1 KiB', '/1/2/3/4');
      });
    });
  });
});
