import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { SortDirection, SortPropDir } from '@swimlane/ngx-datatable';
import * as _ from 'lodash';
import { NodeEvent, Tree, TreeComponent, TreeModel } from 'ng2-tree';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CephfsDir } from '../../../shared/models/cephfs-directory-models';
import { CdDatePipe } from '../../../shared/pipes/cd-date.pipe';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';

@Component({
  selector: 'cd-cephfs-directories',
  templateUrl: './cephfs-directories.component.html',
  styleUrls: ['./cephfs-directories.component.scss']
})
export class CephfsDirectoriesComponent implements OnInit, OnChanges {
  @ViewChild(TreeComponent, { static: true })
  treeComponent: TreeComponent;
  @ViewChild('origin', { static: true })
  originTmpl: TemplateRef<any>;

  @Input()
  id: number;

  private dirs: CephfsDir[];
  private nodeIds: { [path: string]: CephfsDir };
  private requestedPaths: string[];

  selectedDir: CephfsDir;
  tree: TreeModel;
  settings: {
    name: string;
    value: number | string;
    origin: string;
  }[];

  settingsColumns: CdTableColumn[];
  snapshot: { columns: CdTableColumn[]; sortProperties: SortPropDir[] };

  constructor(
    private cephfsService: CephfsService,
    private cdDatePipe: CdDatePipe,
    private i18n: I18n,
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {}

  ngOnInit() {
    this.settingsColumns = [
      {
        prop: 'name',
        name: this.i18n('Name'),
        flexGrow: 1
      },
      {
        prop: 'value',
        name: this.i18n('Value'),
        sortable: false,
        flexGrow: 1
      },
      {
        prop: 'origin',
        name: this.i18n('Origin'),
        sortable: false,
        cellTemplate: this.originTmpl,
        flexGrow: 1
      }
    ];
    this.snapshot = {
      columns: [
        {
          prop: 'name',
          name: this.i18n('Name'),
          flexGrow: 1
        },
        {
          prop: 'path',
          name: this.i18n('Path'),
          isHidden: true,
          flexGrow: 2
        },
        {
          prop: 'created',
          name: this.i18n('Created'),
          flexGrow: 1,
          pipe: this.cdDatePipe
        }
      ],
      sortProperties: [
        {
          dir: SortDirection.desc,
          prop: 'created'
        }
      ]
    };
  }

  ngOnChanges() {
    this.selectedDir = undefined;
    this.dirs = [];
    this.requestedPaths = [];
    this.nodeIds = {};
    if (_.isUndefined(this.id)) {
      this.setRootNode([]);
    } else {
      this.firstCall();
    }
  }

  private setRootNode(nodes: TreeModel[]) {
    const tree: TreeModel = {
      value: '/',
      id: '/',
      settings: {
        selectionAllowed: false,
        static: true
      }
    };
    if (nodes.length > 0) {
      tree.children = nodes;
    }
    this.tree = tree;
  }

  private firstCall() {
    this.updateDirectory('/', (nodes) => this.setRootNode(nodes));
  }

  updateDirectory(path: string, callback: (x: any[]) => void) {
    if (
      !this.requestedPaths.includes(path) &&
      (path === '/' || this.getSubDirectories(path).length > 0)
    ) {
      this.requestedPaths.push(path);
      this.cephfsService
        .lsDir(this.id, path)
        .subscribe((data) => this.loadDirectory(data, path, callback));
    } else {
      this.getChildren(path, callback);
    }
  }

  private getSubDirectories(path: string, tree: CephfsDir[] = this.dirs): CephfsDir[] {
    return tree.filter((d) => d.parent === path);
  }

  private loadDirectory(data: CephfsDir[], path: string, callback: (x: any[]) => void) {
    if (path !== '/') {
      // Removes duplicate directories
      data = data.filter((dir) => dir.parent !== path);
    }
    const dirs = this.dirs.concat(data);
    this.dirs = dirs;
    this.getChildren(path, callback);
  }

  private getChildren(path: string, callback: (x: any[]) => void) {
    const subTree = this.getSubTree(path);
    const nodes = _.sortBy(this.getSubDirectories(path), 'path').map((d) => {
      this.nodeIds[d.path] = d;
      const newNode: TreeModel = {
        value: d.name,
        id: d.path,
        settings: { static: true }
      };
      if (this.getSubDirectories(d.path, subTree).length > 0) {
        // LoadChildren will be triggered if a node is expanded
        newNode.loadChildren = (treeCallback) => this.updateDirectory(d.path, treeCallback);
      }
      return newNode;
    });
    callback(nodes);
  }

  private getSubTree(path: string): CephfsDir[] {
    return this.dirs.filter((d) => d.parent.startsWith(path));
  }

  selectOrigin(path) {
    this.treeComponent.getControllerByNodeId(path).select();
  }

  onNodeSelected(e: NodeEvent) {
    const node = e.node;
    this.treeComponent.getControllerByNodeId(node.id).expand();
    this.setSettings(node);
    this.selectedDir = this.getDirectory(node);
  }

  private setSettings(node: Tree) {
    const files = this.getQuota(node, 'max_files');
    const size = this.getQuota(node, 'max_bytes');
    this.settings = [
      {
        name: 'Max files',
        value: files.value,
        origin: files.origin
      },
      {
        name: 'Max size',
        value: size.value !== '' ? this.dimlessBinaryPipe.transform(size.value) : '',
        origin: size.origin
      }
    ];
  }

  private getQuota(tree: Tree, quotaSetting: string): { value: string; origin: string } {
    tree = this.getOrigin(tree, quotaSetting);
    const dir = this.getDirectory(tree);
    const value = dir.quotas[quotaSetting];
    return {
      value: value ? value : '',
      origin: value ? dir.path : ''
    };
  }

  private getOrigin(tree: Tree, quotaSetting: string): Tree {
    if (tree.parent.value !== '/') {
      const current = this.getQuotaFromTree(tree, quotaSetting);
      const originTree = this.getOrigin(tree.parent, quotaSetting);
      const inherited = this.getQuotaFromTree(originTree, quotaSetting);

      const useOrigin = current === 0 || (inherited !== 0 && inherited < current);
      return useOrigin ? originTree : tree;
    }
    return tree;
  }

  private getQuotaFromTree(tree: Tree, quotaSetting: string): number {
    return this.getDirectory(tree).quotas[quotaSetting];
  }

  private getDirectory(node: Tree): CephfsDir {
    const path = node.id as string;
    return this.nodeIds[path];
  }
}
