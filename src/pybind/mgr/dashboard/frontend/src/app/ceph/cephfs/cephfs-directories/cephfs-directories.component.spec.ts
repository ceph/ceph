import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement, Type } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Validators } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { TreeViewComponent, TreeviewModule } from 'carbon-components-angular';
import { NgbActiveModal, NgbModalModule, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { Observable, of } from 'rxjs';
import _ from 'lodash';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FormModalComponent } from '~/app/shared/components/form-modal/form-modal.component';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import {
  CephfsDir,
  CephfsQuotas,
  CephfsSnapshot
} from '~/app/shared/models/cephfs-directory-models';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, modalServiceShow, PermissionHelper } from '~/testing/unit-test-helper';
import { CephfsDirectoriesComponent } from './cephfs-directories.component';
import { Node } from 'carbon-components-angular/treeview/tree-node.types';
import { By } from '@angular/platform-browser';

describe('CephfsDirectoriesComponent', () => {
  let component: CephfsDirectoriesComponent;
  let fixture: ComponentFixture<CephfsDirectoriesComponent>;
  let cephfsService: CephfsService;
  let noAsyncUpdate: boolean;
  let lsDirSpy: jasmine.Spy;
  let modalShowSpy: jasmine.Spy;
  let notificationShowSpy: jasmine.Spy;
  let minValidator: jasmine.Spy;
  let maxValidator: jasmine.Spy;
  let minBinaryValidator: jasmine.Spy;
  let maxBinaryValidator: jasmine.Spy;
  let modal: NgbModalRef;
  let treeComponent: DebugElement;
  let testUsedQuotas: boolean;

  // Get's private attributes or functions
  const get = {
    nodeIds: (): { [path: string]: CephfsDir } => component['nodeIds'],
    dirs: (): CephfsDir[] => component['dirs'],
    requestedPaths: (): string[] => component['requestedPaths']
  };

  // Object contains mock data that will be reset before each test.
  let mockData: {
    nodes: Node[];
    parent: any;
    createdSnaps: CephfsSnapshot[] | any[];
    deletedSnaps: CephfsSnapshot[] | any[];
    updatedQuotas: { [path: string]: CephfsQuotas };
    createdDirs: CephfsDir[];
  };

  // Object contains mock functions
  const mockLib = {
    quotas: (max_bytes: number, max_files: number): CephfsQuotas => ({ max_bytes, max_files }),
    snapshots: (dirPath: string, howMany: number): CephfsSnapshot[] => {
      const name = 'someSnapshot';
      const snapshots = [];
      const oneDay = 3600 * 24 * 1000;
      for (let i = 0; i < howMany; i++) {
        const snapName = `${name}${i + 1}`;
        const path = `${dirPath}/.snap/${snapName}`;
        const created = new Date(+new Date() - oneDay * i).toString();
        snapshots.push({ name: snapName, path, created });
      }
      return snapshots;
    },
    dir: (parentPath: string, name: string, modifier: number): CephfsDir => {
      const dirPath = `${parentPath === '/' ? '' : parentPath}/${name}`;
      let snapshots = mockLib.snapshots(parentPath, modifier);
      const extraSnapshots = mockData.createdSnaps.filter((s) => s.path === dirPath);
      if (extraSnapshots.length > 0) {
        snapshots = snapshots.concat(extraSnapshots);
      }
      const deletedSnapshots = mockData.deletedSnaps
        .filter((s) => s.path === dirPath)
        .map((s) => s.name);
      if (deletedSnapshots.length > 0) {
        snapshots = snapshots.filter((s) => !deletedSnapshots.includes(s.name));
      }
      return {
        name,
        path: dirPath,
        parent: parentPath,
        quotas: Object.assign(
          mockLib.quotas(1024 * modifier, 10 * modifier),
          mockData.updatedQuotas[dirPath] || {}
        ),
        snapshots: snapshots
      };
    },
    // Only used inside other mocks
    lsSingleDir: (
      path = '',
      names: any = [
        { name: 'c', modifier: 3 },
        { name: 'a', modifier: 1 },
        { name: 'b', modifier: 2 }
      ]
    ): CephfsDir[] => {
      const customDirs = mockData.createdDirs.filter((d) => d.parent === path);
      const isCustomDir = mockData.createdDirs.some((d) => d.path === path);
      if (isCustomDir || path.includes('b')) {
        // 'b' has no sub directories
        return customDirs;
      }
      return customDirs.concat(
        // Directories are not sorted!
        names.map((x: any) => mockLib.dir(x?.path || path, x.name, x.modifier))
      );
    },
    lsDir: (_id: number, path = ''): Observable<CephfsDir[]> => {
      // will return 2 levels deep
      let data = mockLib.lsSingleDir(path);

      if (testUsedQuotas) {
        const parents = mockLib.lsSingleDir(path, [
          { name: 'c', modifier: 3 },
          { name: 'a', modifier: 1 },
          { name: 'b', modifier: 2 },
          { path: '', name: '1', modifier: 1 },
          { path: '/1', name: '2', modifier: 1 },
          { path: '/1/2', name: '3', modifier: 1 }
        ]);
        data = data.concat(parents);
      }
      const paths = data.map((dir) => dir.path);
      paths.forEach((pathL2) => {
        data = data.concat(mockLib.lsSingleDir(pathL2));
      });
      if (path === '' || path === '/') {
        // Adds root directory on ls of '/' to the directories list.
        const root = mockLib.dir(path, '/', 1);
        root.path = '/';
        root.parent = undefined;
        root.quotas = undefined;
        data = [root].concat(data);
      }
      return of(data);
    },
    mkSnapshot: (_id: any, path: string, name: string): Observable<string> => {
      mockData.createdSnaps.push({
        name,
        path,
        created: new Date().toString()
      });
      return of(name);
    },
    rmSnapshot: (_id: any, path: string, name: string): Observable<string> => {
      mockData.deletedSnaps.push({
        name,
        path,
        created: new Date().toString()
      });
      return of(name);
    },
    updateQuota: (_id: any, path: string, updated: CephfsQuotas): Observable<string> => {
      mockData.updatedQuotas[path] = Object.assign(mockData.updatedQuotas[path] || {}, updated);
      return of('Response');
    },
    modalShow: (comp: Type<any>, init: any): any => {
      modal = modalServiceShow(comp, init);
      return modal;
    },
    getNodeById: (path: string) => {
      return mockLib.useNode(path);
    },
    updateNodes: (path: string) => {
      // const p: Promise<any[]> = component.treeOptions.getChildren({ id: path });
      const p: Promise<Node[]> = component.updateDirectory(path);
      return noAsyncUpdate ? () => p : mockLib.asyncNodeUpdate(p);
    },
    asyncNodeUpdate: fakeAsync((p: Promise<any[]>) => {
      p?.then((nodes) => {
        mockData.nodes = mockData.nodes.concat(nodes);
      });
      tick();
    }),
    flattenTree: (tree: Node[], memoised: Node[] = []) => {
      let result = memoised;
      tree.some((node) => {
        result = [node, ...mockLib.flattenTree(node?.children || [], result)];
      });
      return _.sortBy(result, 'id');
    },
    changeId: (id: number) => {
      component.id = id;
      component.ngOnChanges();
      mockData.nodes = mockLib.flattenTree(component.nodes).concat(mockData.nodes);
    },
    selectNode: (path: string) => {
      // component.treeOptions.actionMapping.mouse.click(undefined, mockLib.useNode(path), undefined);
      const node = mockLib.useNode(path);
      component.selectNode(node);
    },
    // Creates TreeNode with parents until root
    useNode: (path: string): Node => {
      const parentPath = path.split('/');
      parentPath.pop();
      const parentIsRoot = parentPath.length === 1;
      const parent = parentIsRoot ? { id: '/' } : mockLib.useNode(parentPath.join('/'));
      return {
        id: path,
        label: path,
        name: path,
        value: { parent: parent?.id }
      };
    },
    treeActions: {
      toggleActive: (node: Node) => {
        return mockLib.updateNodes(node.id);
      }
    },
    mkDir: (path: string, name: string, maxFiles: number, maxBytes: number) => {
      const dir = mockLib.dir(path, name, 3);
      dir.quotas.max_bytes = maxBytes * 1024;
      dir.quotas.max_files = maxFiles;
      mockData.createdDirs.push(dir);
      // Below is needed for quota tests only where 4 dirs are mocked
      get.nodeIds()[dir.path] = dir;
      const node = mockLib.useNode(dir.path);
      mockData.nodes.push(node);
    },
    createSnapshotThroughModal: (name: string) => {
      component.createSnapshot();
      modal.componentInstance.onSubmitForm({ name });
    },
    deleteSnapshotsThroughModal: (snapshots: CephfsSnapshot[]) => {
      component.snapshot.selection.selected = snapshots;
      component.deleteSnapshotModal();
      modal.componentInstance.callSubmitAction();
    },
    updateQuotaThroughModal: (attribute: string, value: number) => {
      component.quota.selection.selected = component.settings.filter(
        (q) => q.quotaKey === attribute
      );
      component.updateQuotaModal();
      modal.componentInstance.onSubmitForm({ [attribute]: value });
    },
    unsetQuotaThroughModal: (attribute: string) => {
      component.quota.selection.selected = component.settings.filter(
        (q) => q.quotaKey === attribute
      );
      component.unsetQuotaModal();
      modal.componentInstance.onSubmit();
    },
    setFourQuotaDirs: (quotas: number[][]) => {
      expect(quotas.length).toBe(4); // Make sure this function is used correctly
      let path = '';
      quotas.forEach((quota, index) => {
        index += 1;
        mockLib.mkDir(path === '' ? '/' : path, index.toString(), quota[0], quota[1]);
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
      };
      mockLib.selectNode('/1/2/3/4');
    }
  };

  // Expects that are used frequently
  const assert = {
    dirLength: (n: number) => expect(get.dirs().length).toBe(n),
    nodeLength: (n: number) => expect(mockData.nodes?.length).toBe(n),
    lsDirCalledTimes: (n: number) => expect(lsDirSpy).toHaveBeenCalledTimes(n),
    lsDirHasBeenCalledWith: (id: number, paths: string[]) => {
      paths.forEach((path) => expect(lsDirSpy).toHaveBeenCalledWith(id, path));
      assert.lsDirCalledTimes(paths.length);
    },
    requestedPaths: (expected: string[]) => expect(get.requestedPaths()).toEqual(expected),
    snapshotsByName: (snaps: string[]) =>
      expect(component.selectedDir.snapshots.map((s) => s.name)).toEqual(snaps),
    dirQuotas: (bytes: number, files: number) => {
      expect(component.selectedDir.quotas).toEqual({ max_bytes: bytes, max_files: files });
    },
    noQuota: (key: 'bytes' | 'files') => {
      assert.quotaRow(key, '', 0, '');
    },
    quotaIsNotInherited: (key: 'bytes' | 'files', shownValue: any, nextMaximum: number) => {
      const dir = component.selectedDir;
      const path = dir.path;
      assert.quotaRow(key, shownValue, nextMaximum, path);
    },
    quotaIsInherited: (key: 'bytes' | 'files', shownValue: any, path: string) => {
      const isBytes = key === 'bytes';
      const nextMaximum = get.nodeIds()[path].quotas[isBytes ? 'max_bytes' : 'max_files'];
      assert.quotaRow(key, shownValue, nextMaximum, path);
    },
    quotaRow: (
      key: 'bytes' | 'files',
      shownValue: number | string,
      nextTreeMaximum: number,
      originPath: string
    ) => {
      const isBytes = key === 'bytes';
      expect(component.settings[isBytes ? 1 : 0]).toEqual({
        row: {
          name: `Max ${isBytes ? 'size' : key}`,
          value: shownValue,
          originPath
        },
        quotaKey: `max_${key}`,
        dirValue: expect.any(Number),
        nextTreeMaximum: {
          value: nextTreeMaximum,
          path: expect.any(String)
        }
      });
    },
    quotaUnsetModalTexts: (titleText: string, message: string, notificationMsg: string) => {
      expect(modalShowSpy).toHaveBeenCalledWith(
        ConfirmationModalComponent,
        expect.objectContaining({
          titleText,
          description: message,
          buttonText: 'Unset'
        })
      );
      expect(notificationShowSpy).toHaveBeenCalledWith(NotificationType.success, notificationMsg);
    },
    quotaUpdateModalTexts: (titleText: string, message: string, notificationMsg: string) => {
      expect(modalShowSpy).toHaveBeenCalledWith(
        FormModalComponent,
        expect.objectContaining({
          titleText,
          message,
          submitButtonText: 'Save'
        })
      );
      expect(notificationShowSpy).toHaveBeenCalledWith(NotificationType.success, notificationMsg);
    },
    quotaUpdateModalField: (
      type: string,
      label: string,
      key: string,
      value: number,
      max: number,
      errors?: { [key: string]: string }
    ) => {
      expect(modalShowSpy).toHaveBeenCalledWith(
        FormModalComponent,
        expect.objectContaining({
          fields: [
            {
              type,
              label,
              errors,
              name: key,
              value,
              validators: expect.anything(),
              required: true
            }
          ]
        })
      );
      if (type === 'binary') {
        expect(minBinaryValidator).toHaveBeenCalledWith(0);
        expect(maxBinaryValidator).toHaveBeenCalledWith(max);
      } else {
        expect(minValidator).toHaveBeenCalledWith(0);
        expect(maxValidator).toHaveBeenCalledWith(max);
      }
    }
  };

  configureTestBed(
    {
      imports: [
        HttpClientTestingModule,
        SharedModule,
        RouterTestingModule,
        TreeviewModule,
        ToastrModule.forRoot(),
        NgbModalModule
      ],
      declarations: [CephfsDirectoriesComponent],
      providers: [NgbActiveModal]
    },
    [CriticalConfirmationModalComponent, FormModalComponent, ConfirmationModalComponent]
  );

  beforeEach(() => {
    noAsyncUpdate = false;
    mockData = {
      nodes: [],
      parent: undefined,
      createdSnaps: [],
      deletedSnaps: [],
      createdDirs: [],
      updatedQuotas: {}
    };

    cephfsService = TestBed.inject(CephfsService);
    lsDirSpy = spyOn(cephfsService, 'lsDir').and.callFake(mockLib.lsDir);
    spyOn(cephfsService, 'mkSnapshot').and.callFake(mockLib.mkSnapshot);
    spyOn(cephfsService, 'rmSnapshot').and.callFake(mockLib.rmSnapshot);
    spyOn(cephfsService, 'quota').and.callFake(mockLib.updateQuota);
    spyOn(global, 'setTimeout').and.callFake((fn) => fn());

    modalShowSpy = spyOn(TestBed.inject(ModalService), 'show').and.callFake(mockLib.modalShow);
    notificationShowSpy = spyOn(TestBed.inject(NotificationService), 'show').and.stub();

    fixture = TestBed.createComponent(CephfsDirectoriesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    treeComponent = fixture.debugElement.query(By.directive(TreeViewComponent));

    // spyOn(TREE_ACTIONS, 'TOGGLE_ACTIVE').and.callFake(mockLib.treeActions.toggleActive);
    // spyOn(component, 'selectNode').and.callFake(mockLib.treeActions.toggleActive);
    // spyOn(component, 'getNode').and.callFake(mockLib.useNode);

    component.treeComponent = treeComponent.componentInstance as TreeViewComponent;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('mock self test', () => {
    it('tests snapshots mock', () => {
      expect(mockLib.snapshots('/a', 1).map((s) => ({ name: s.name, path: s.path }))).toEqual([
        {
          name: 'someSnapshot1',
          path: '/a/.snap/someSnapshot1'
        }
      ]);
      expect(mockLib.snapshots('/a/b', 3).map((s) => ({ name: s.name, path: s.path }))).toEqual([
        {
          name: 'someSnapshot1',
          path: '/a/b/.snap/someSnapshot1'
        },
        {
          name: 'someSnapshot2',
          path: '/a/b/.snap/someSnapshot2'
        },
        {
          name: 'someSnapshot3',
          path: '/a/b/.snap/someSnapshot3'
        }
      ]);
    });

    it('tests dir mock', () => {
      const path = '/a/b/c';
      mockData.createdSnaps = [
        { path, name: 's1' },
        { path, name: 's2' }
      ];
      mockData.deletedSnaps = [
        { path, name: 'someSnapshot2' },
        { path, name: 's2' }
      ];
      const dir = mockLib.dir('/a/b', 'c', 2);
      expect(dir.path).toBe('/a/b/c');
      expect(dir.parent).toBe('/a/b');
      expect(dir.quotas).toEqual({ max_bytes: 2048, max_files: 20 });
      expect(dir.snapshots.map((s) => s.name)).toEqual(['someSnapshot1', 's1']);
    });

    it('tests lsdir mock', () => {
      let dirs: CephfsDir[] = [];
      mockLib.lsDir(2, '/a').subscribe((x) => (dirs = x));
      expect(dirs.map((d) => d.path)).toEqual([
        '/a/c',
        '/a/a',
        '/a/b',
        '/a/c/c',
        '/a/c/a',
        '/a/c/b',
        '/a/a/c',
        '/a/a/a',
        '/a/a/b'
      ]);
    });

    describe('test quota update mock', () => {
      const PATH = '/a';
      const ID = 2;

      const updateQuota = (quotas: CephfsQuotas) => mockLib.updateQuota(ID, PATH, quotas);

      const expectMockUpdate = (max_bytes?: number, max_files?: number) =>
        expect(mockData.updatedQuotas[PATH]).toEqual({
          max_bytes,
          max_files
        });

      const expectLsUpdate = (max_bytes?: number, max_files?: number) => {
        let dir: CephfsDir;
        mockLib.lsDir(ID, '/').subscribe((dirs) => (dir = dirs.find((d) => d.path === PATH)));
        expect(dir.quotas).toEqual({
          max_bytes,
          max_files
        });
      };

      it('tests to set quotas', () => {
        expectLsUpdate(1024, 10);

        updateQuota({ max_bytes: 512 });
        expectMockUpdate(512);
        expectLsUpdate(512, 10);

        updateQuota({ max_files: 100 });
        expectMockUpdate(512, 100);
        expectLsUpdate(512, 100);
      });

      it('tests to unset quotas', () => {
        updateQuota({ max_files: 0 });
        expectMockUpdate(undefined, 0);
        expectLsUpdate(1024, 0);

        updateQuota({ max_bytes: 0 });
        expectMockUpdate(0, 0);
        expectLsUpdate(0, 0);
      });
    });
  });

  it('calls lsDir only if an id exits', () => {
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
      const actual = mockData.nodes.map((node: Node) => ({
        [node.id]: node?.expanded || Boolean(node?.children?.length)
      }));
      const expected = [
        {
          '/': true
        },
        {
          '/a': true
        },
        {
          '/a/a': false
        },
        {
          '/a/b': false
        },
        {
          '/a/c': false
        },
        {
          '/b': false
        },
        {
          '/c': true
        },
        {
          '/c/a': false
        },
        {
          '/c/b': false
        },
        {
          '/c/c': false
        }
      ];
      expect(actual).toEqual(expected);
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
      assert.nodeLength(10);
      assert.dirLength(16);
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
      assert.quotaIsNotInherited('files', 10, 0);
      assert.quotaIsNotInherited('bytes', '1 KiB', 0);
    });

    it('should extend the list by subdirectories when expanding', () => {
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
      assert.dirLength(22);
      assert.nodeLength(10);
    });

    it('should update the tree after each selection', () => {
      const spy = spyOn(component, 'selectNode').and.callThrough();
      expect(spy).toHaveBeenCalledTimes(0);
      mockLib.selectNode('/a');
      expect(spy).toHaveBeenCalledTimes(1);
      mockLib.selectNode('/a/c');
      expect(spy).toHaveBeenCalledTimes(2);
    });

    it('should select parent by path', () => {
      mockLib.selectNode('/a');
      mockLib.selectNode('/a/c');
      mockLib.selectNode('/a/c/a');
      component.selectOrigin('/a');
      console.debug('component.selectedDir', component.selectedDir);
      expect(component.selectedDir.path).toBe('/a');
    });

    it('should refresh directories with no sub directories as they could have some now', () => {
      mockLib.selectNode('/b');
      /**
       * Tree looks like this:
       * v /
       *   > a
       *   * b <- Selected
       *   > c
       * */
      assert.lsDirCalledTimes(2);
      assert.requestedPaths(['/', '/b']);
      assert.nodeLength(10);
    });

    describe('used quotas', () => {
      beforeAll(() => {
        testUsedQuotas = true;
      });

      afterAll(() => {
        testUsedQuotas = false;
      });

      it('should use no quota if none is set', () => {
        mockLib.setFourQuotaDirs([
          [0, 0],
          [0, 0],
          [0, 0],
          [0, 0]
        ]);
        assert.noQuota('files');
        assert.noQuota('bytes');
        assert.dirQuotas(0, 0);
      });

      it('should use quota from upper parents', () => {
        mockLib.setFourQuotaDirs([
          [100, 0],
          [0, 8],
          [0, 0],
          [0, 0]
        ]);
        assert.quotaIsInherited('files', 100, '/1');
        assert.quotaIsInherited('bytes', '8 KiB', '/1/2');
        assert.dirQuotas(0, 0);
      });

      it('should use quota from the parent with the lowest value (deep inheritance)', () => {
        mockLib.setFourQuotaDirs([
          [200, 1],
          [100, 4],
          [400, 3],
          [300, 2]
        ]);
        assert.quotaIsInherited('files', 100, '/1/2');
        assert.quotaIsInherited('bytes', '1 KiB', '/1');
        assert.dirQuotas(2048, 300);
      });

      it('should use current value', () => {
        mockLib.setFourQuotaDirs([
          [200, 2],
          [300, 4],
          [400, 3],
          [100, 1]
        ]);
        assert.quotaIsNotInherited('files', 100, 200);
        assert.quotaIsNotInherited('bytes', '1 KiB', 2048);
        assert.dirQuotas(1024, 100);
      });
    });
  });

  // skipping this since cds-modal is currently not testable
  // within the unit tests because of the absence of placeholder7
  describe.skip('snapshots', () => {
    beforeEach(() => {
      mockLib.changeId(1);
      mockLib.selectNode('/a');
    });

    it('should create a snapshot', () => {
      mockLib.createSnapshotThroughModal('newSnap');
      expect(cephfsService.mkSnapshot).toHaveBeenCalledWith(1, '/a', 'newSnap');
      assert.snapshotsByName(['someSnapshot1', 'newSnap']);
    });

    it('should delete a snapshot', () => {
      mockLib.createSnapshotThroughModal('deleteMe');
      mockLib.deleteSnapshotsThroughModal([component.selectedDir.snapshots[1]]);
      assert.snapshotsByName(['someSnapshot1']);
    });

    it('should delete all snapshots', () => {
      mockLib.createSnapshotThroughModal('deleteAll');
      mockLib.deleteSnapshotsThroughModal(component.selectedDir.snapshots);
      assert.snapshotsByName([]);
    });
  });

  // Need to change PermissionHelper to reflect latest changes to table actions component
  it.skip('should test all snapshot table actions combinations', () => {
    const permissionHelper: PermissionHelper = new PermissionHelper(component.permission);
    const tableActions = permissionHelper.setPermissionsAndGetActions(
      component.snapshot.tableActions
    );

    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: ['Create', 'Delete'],
        primary: { multiple: 'Delete', executing: 'Delete', single: 'Delete', no: 'Create' }
      },
      'create,update': {
        actions: ['Create'],
        primary: { multiple: 'Create', executing: 'Create', single: 'Create', no: 'Create' }
      },
      'create,delete': {
        actions: ['Create', 'Delete'],
        primary: { multiple: 'Delete', executing: 'Delete', single: 'Delete', no: 'Create' }
      },
      create: {
        actions: ['Create'],
        primary: { multiple: 'Create', executing: 'Create', single: 'Create', no: 'Create' }
      },
      'update,delete': {
        actions: ['Delete'],
        primary: { multiple: 'Delete', executing: 'Delete', single: 'Delete', no: 'Delete' }
      },
      update: {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      },
      delete: {
        actions: ['Delete'],
        primary: { multiple: 'Delete', executing: 'Delete', single: 'Delete', no: 'Delete' }
      },
      'no-permissions': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      }
    });
  });

  // skipping this since cds-modal is currently not testable
  // within the unit tests because of the absence of placeholder
  describe.skip('quotas', () => {
    beforeEach(() => {
      // Spies
      minValidator = spyOn(Validators, 'min').and.callThrough();
      maxValidator = spyOn(Validators, 'max').and.callThrough();
      minBinaryValidator = spyOn(CdValidators, 'binaryMin').and.callThrough();
      maxBinaryValidator = spyOn(CdValidators, 'binaryMax').and.callThrough();
      // Select /a/c/b
      mockLib.changeId(1);
      mockLib.selectNode('/a');
      mockLib.selectNode('/a/c');
      mockLib.selectNode('/a/c/b');
      // Quotas after selection
      assert.quotaIsInherited('files', 10, '/a');
      assert.quotaIsInherited('bytes', '1 KiB', '/a');
      assert.dirQuotas(2048, 20);
    });

    describe('update modal', () => {
      describe('max_files', () => {
        beforeEach(() => {
          mockLib.updateQuotaThroughModal('max_files', 5);
        });

        it('should update max_files correctly', () => {
          expect(cephfsService.quota).toHaveBeenCalledWith(1, '/a/c/b', { max_files: 5 });
          assert.quotaIsNotInherited('files', 5, 10);
        });

        it('uses the correct form field', () => {
          assert.quotaUpdateModalField('number', 'Max files', 'max_files', 20, 10, {
            min: 'Value has to be at least 0 or more',
            max: 'Value has to be at most 10 or less'
          });
        });

        it('shows the right texts', () => {
          assert.quotaUpdateModalTexts(
            `Update CephFS files quota for '/a/c/b'`,
            `The inherited files quota 10 from '/a' is the maximum value to be used.`,
            `Updated CephFS files quota for '/a/c/b'`
          );
        });
      });

      describe('max_bytes', () => {
        beforeEach(() => {
          mockLib.updateQuotaThroughModal('max_bytes', 512);
        });

        it('should update max_files correctly', () => {
          expect(cephfsService.quota).toHaveBeenCalledWith(1, '/a/c/b', { max_bytes: 512 });
          assert.quotaIsNotInherited('bytes', '512 B', 1024);
        });

        it('uses the correct form field', () => {
          mockLib.updateQuotaThroughModal('max_bytes', 512);
          assert.quotaUpdateModalField('binary', 'Max size', 'max_bytes', 2048, 1024);
        });

        it('shows the right texts', () => {
          assert.quotaUpdateModalTexts(
            `Update CephFS size quota for '/a/c/b'`,
            `The inherited size quota 1 KiB from '/a' is the maximum value to be used.`,
            `Updated CephFS size quota for '/a/c/b'`
          );
        });
      });

      describe('action behaviour', () => {
        it('opens with next maximum as maximum if directory holds the current maximum', () => {
          mockLib.updateQuotaThroughModal('max_bytes', 512);
          mockLib.updateQuotaThroughModal('max_bytes', 888);
          assert.quotaUpdateModalField('binary', 'Max size', 'max_bytes', 512, 1024);
        });

        it(`uses 'Set' action instead of 'Update' if the quota is not set (0)`, () => {
          mockLib.updateQuotaThroughModal('max_bytes', 0);
          mockLib.updateQuotaThroughModal('max_bytes', 200);
          assert.quotaUpdateModalTexts(
            `Set CephFS size quota for '/a/c/b'`,
            `The inherited size quota 1 KiB from '/a' is the maximum value to be used.`,
            `Set CephFS size quota for '/a/c/b'`
          );
        });
      });
    });

    describe('unset modal', () => {
      describe('max_files', () => {
        beforeEach(() => {
          mockLib.updateQuotaThroughModal('max_files', 5); // Sets usable quota
          mockLib.unsetQuotaThroughModal('max_files');
        });

        it('should unset max_files correctly', () => {
          expect(cephfsService.quota).toHaveBeenCalledWith(1, '/a/c/b', { max_files: 0 });
          assert.dirQuotas(2048, 0);
        });

        it('shows the right texts', () => {
          assert.quotaUnsetModalTexts(
            `Unset CephFS files quota for '/a/c/b'`,
            `Unset files quota 5 from '/a/c/b' in order to inherit files quota 10 from '/a'.`,
            `Unset CephFS files quota for '/a/c/b'`
          );
        });
      });

      describe('max_bytes', () => {
        beforeEach(() => {
          mockLib.updateQuotaThroughModal('max_bytes', 512); // Sets usable quota
          mockLib.unsetQuotaThroughModal('max_bytes');
        });

        it('should unset max_files correctly', () => {
          expect(cephfsService.quota).toHaveBeenCalledWith(1, '/a/c/b', { max_bytes: 0 });
          assert.dirQuotas(0, 20);
        });

        it('shows the right texts', () => {
          assert.quotaUnsetModalTexts(
            `Unset CephFS size quota for '/a/c/b'`,
            `Unset size quota 512 B from '/a/c/b' in order to inherit size quota 1 KiB from '/a'.`,
            `Unset CephFS size quota for '/a/c/b'`
          );
        });
      });

      describe('action behaviour', () => {
        it('uses different Text if no quota is inherited', () => {
          mockLib.selectNode('/a');
          mockLib.unsetQuotaThroughModal('max_bytes');
          assert.quotaUnsetModalTexts(
            `Unset CephFS size quota for '/a'`,
            `Unset size quota 1 KiB from '/a' in order to have no quota on the directory.`,
            `Unset CephFS size quota for '/a'`
          );
        });

        it('uses different Text if quota is already inherited', () => {
          mockLib.unsetQuotaThroughModal('max_bytes');
          assert.quotaUnsetModalTexts(
            `Unset CephFS size quota for '/a/c/b'`,
            `Unset size quota 2 KiB from '/a/c/b' which isn't used because of the inheritance ` +
              `of size quota 1 KiB from '/a'.`,
            `Unset CephFS size quota for '/a/c/b'`
          );
        });
      });
    });
  });

  describe('table actions', () => {
    let actions: CdTableAction[];

    const empty = (): CdTableSelection => new CdTableSelection();

    const select = (value: number): CdTableSelection => {
      const selection = new CdTableSelection();
      selection.selected = [{ dirValue: value }];
      return selection;
    };

    beforeEach(() => {
      actions = component.quota.tableActions;
    });

    it(`shows 'Set' for empty and not set quotas`, () => {
      const isSetVisible = actions[0].visible;
      expect(isSetVisible(empty())).toBe(true);
      expect(isSetVisible(select(0))).toBe(true);
      expect(isSetVisible(select(1))).toBe(false);
    });

    it(`shows 'Update' for set quotas only`, () => {
      const isUpdateVisible = actions[1].visible;
      expect(isUpdateVisible(empty())).toBeFalsy();
      expect(isUpdateVisible(select(0))).toBe(false);
      expect(isUpdateVisible(select(1))).toBe(true);
    });

    it(`only enables 'Unset' for set quotas only`, () => {
      const isUnsetDisabled = actions[2].disable;
      expect(isUnsetDisabled(empty())).toBe(true);
      expect(isUnsetDisabled(select(0))).toBe(true);
      expect(isUnsetDisabled(select(1))).toBe(false);
    });

    // Need to change PermissionHelper to reflect latest changes to table actions component
    it.skip('should test all quota table actions permission combinations', () => {
      const permissionHelper: PermissionHelper = new PermissionHelper(component.permission, {
        single: { dirValue: 0 },
        multiple: [{ dirValue: 0 }, {}]
      });
      const tableActions = permissionHelper.setPermissionsAndGetActions(
        component.quota.tableActions
      );

      expect(tableActions).toEqual({
        'create,update,delete': {
          actions: ['Set', 'Update', 'Unset'],
          primary: { multiple: 'Set', executing: 'Set', single: 'Set', no: 'Set' }
        },
        'create,update': {
          actions: ['Set', 'Update', 'Unset'],
          primary: { multiple: 'Set', executing: 'Set', single: 'Set', no: 'Set' }
        },
        'create,delete': {
          actions: [],
          primary: { multiple: '', executing: '', single: '', no: '' }
        },
        create: {
          actions: [],
          primary: { multiple: '', executing: '', single: '', no: '' }
        },
        'update,delete': {
          actions: ['Set', 'Update', 'Unset'],
          primary: { multiple: 'Set', executing: 'Set', single: 'Set', no: 'Set' }
        },
        update: {
          actions: ['Set', 'Update', 'Unset'],
          primary: { multiple: 'Set', executing: 'Set', single: 'Set', no: 'Set' }
        },
        delete: {
          actions: [],
          primary: { multiple: '', executing: '', single: '', no: '' }
        },
        'no-permissions': {
          actions: [],
          primary: { multiple: '', executing: '', single: '', no: '' }
        }
      });
    });
  });

  describe('reload all', () => {
    const calledPaths = ['/', '/a', '/a/c', '/a/c/a', '/a/c/a/b'];

    const dirsByPath = (): string[] => get.dirs().map((d) => d.path);

    beforeEach(() => {
      mockLib.changeId(1);
      mockLib.selectNode('/a');
      mockLib.selectNode('/a/c');
      mockLib.selectNode('/a/c/a');
      mockLib.selectNode('/a/c/a/b');
    });

    it('should reload all requested paths', () => {
      assert.lsDirHasBeenCalledWith(1, calledPaths);
      lsDirSpy.calls.reset();
      assert.lsDirHasBeenCalledWith(1, []);
      // component.refreshAllDirectories();
      // assert.lsDirHasBeenCalledWith(1, calledPaths);
    });

    it('should reload all requested paths if not selected anything', () => {
      lsDirSpy.calls.reset();
      mockLib.changeId(2);
      assert.lsDirHasBeenCalledWith(2, ['/']);
      lsDirSpy.calls.reset();
      component.refreshAllDirectories();
      lsDirSpy.calls.reset();
      mockLib.changeId(2);
      assert.lsDirHasBeenCalledWith(2, ['/']);
    });

    it('should add new directories', () => {
      // Create two new directories in preparation
      const dirsBeforeRefresh = dirsByPath();
      expect(dirsBeforeRefresh.includes('/a/c/has_dir_now')).toBe(false);
      mockLib.mkDir('/a/c', 'has_dir_now', 0, 0);
      mockLib.mkDir('/a/c/a/b', 'has_dir_now_too', 0, 0);
      // Now the new directories will be fetched
      component.refreshAllDirectories();
      const dirsAfterRefresh = dirsByPath();
      expect(dirsAfterRefresh.length - dirsBeforeRefresh.length).toBe(2);
      expect(dirsAfterRefresh.includes('/a/c/has_dir_now')).toBe(true);
      expect(dirsAfterRefresh.includes('/a/c/a/b/has_dir_now_too')).toBe(true);
    });

    it('should remove deleted directories', () => {
      // Create one new directory and refresh in order to have it added to the directories list
      mockLib.mkDir('/a/c', 'will_be_removed_shortly', 0, 0);
      component.refreshAllDirectories();
      const dirsBeforeRefresh = dirsByPath();
      expect(dirsBeforeRefresh.includes('/a/c/will_be_removed_shortly')).toBe(true);
      mockData.createdDirs = []; // Mocks the deletion of the directory
      // Now the deleted directory will be missing on refresh
      component.refreshAllDirectories();
      const dirsAfterRefresh = dirsByPath();
      expect(dirsAfterRefresh.length - dirsBeforeRefresh.length).toBe(-1);
      expect(dirsAfterRefresh.includes('/a/c/will_be_removed_shortly')).toBe(false);
    });

    describe('loading indicator', () => {
      beforeEach(() => {
        noAsyncUpdate = true;
      });

      it('should have set loading indicator to false after refreshing all dirs', fakeAsync(() => {
        component.refreshAllDirectories();
        expect(component.loadingIndicator).toBe(true);
        tick(3000); // To resolve all promises
        expect(component.loadingIndicator).toBe(false);
      }));

      it('should have set all loaded dirs as attribute names of "indicators"', () => {
        noAsyncUpdate = false;
        component.refreshAllDirectories();
        expect(Object.keys(component.loading).sort()).toEqual(calledPaths);
      });

      it('should set an indicator to true during load', () => {
        lsDirSpy.and.callFake(() => new Observable((): null => null));
        component.refreshAllDirectories();
        expect(
          Object.keys(component.loading)
            .filter((x) => x !== '/')
            .every((key) => component.loading[key])
        ).toBe(true);
      });
    });
    describe('disable create snapshot', () => {
      let actions: CdTableAction[];
      beforeEach(() => {
        actions = component.snapshot.tableActions;
        mockLib.mkDir('/', 'volumes', 2, 2);
        mockLib.mkDir('/volumes', 'group1', 2, 2);
        mockLib.mkDir('/volumes/group1', 'subvol', 2, 2);
        mockLib.mkDir('/volumes/group1/subvol', 'subfile', 2, 2);
      });

      const empty = (): CdTableSelection => new CdTableSelection();

      it('should return a descriptive message to explain why it is disabled', () => {
        const path = '/volumes/group1/subvol/subfile';
        const res = 'Cannot create snapshots for files/folders in the subvolume subvol';
        mockLib.selectNode(path);
        expect(actions[0].disable(empty())).toContain(res);
      });

      it('should return false if it is not a subvolume node', () => {
        const testCases = [
          '/volumes/group1/subvol',
          '/volumes/group1',
          '/volumes',
          '/',
          '/a',
          '/a/b'
        ];
        testCases.forEach((testCase) => {
          mockLib.selectNode(testCase);
          expect(actions[0].disable(empty())).toBeFalsy();
        });
      });
    });
  });

  describe('tree node helper methods', () => {
    describe('getParent', () => {
      it('should return the parent node for a given path', () => {
        const dirs: CephfsDir[] = [
          mockLib.dir('/', 'parent', 2),
          mockLib.dir('/parent', 'some', 2)
        ];

        const parentNode = component.getParent(dirs, '/parent');

        expect(parentNode).not.toBeNull();
        expect(parentNode?.id).toEqual('/parent');
        expect(parentNode?.label).toEqual('parent');
        expect(parentNode?.value?.parent).toEqual('/');
      });

      it('should return null if no parent node is found', () => {
        const dirs: CephfsDir[] = [mockLib.dir('/', 'no parent', 2)];

        const parentNode = component.getParent(dirs, '/some/other/path');

        expect(parentNode).toBeNull();
      });

      it('should handle an empty dirs array', () => {
        const dirs: CephfsDir[] = [];

        const parentNode = component.getParent(dirs, '/some/path');

        expect(parentNode).toBeNull();
      });
    });

    describe('toNode', () => {
      it('should convert a CephfsDir to a Node', () => {
        const directory: CephfsDir = mockLib.dir('/some/parent', '/some/path', 2);

        const node: Node = component.toNode(directory);

        expect(node.id).toEqual(directory.path);
        expect(node.label).toEqual(directory.name);
        expect(node.children).toEqual([]);
        expect(node.expanded).toBe(false);
        expect(node.value).toEqual({ parent: directory.parent });
      });

      it('should handle a CephfsDir with no parent', () => {
        const directory: CephfsDir = mockLib.dir(undefined, '/some/path', 2);

        const node: Node = component.toNode(directory);

        expect(node.value).toEqual({ parent: undefined });
      });
    });
  });
});
