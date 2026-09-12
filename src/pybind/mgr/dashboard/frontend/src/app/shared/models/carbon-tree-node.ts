/**
 * The type is referred from https://github.com/carbon-design-system/carbon-components-angular/blob/v5.59.2/src/treeview/tree-node.types.ts
 * The type is copied here to avoid the issue of importing from the node_modules folder.
 * The new esbuild & package exports of Angular 20 prevent us from relying
 * on internal library files that might change without warning. So if we need to rely on the Node
 * this type should be officially exported by the carbon component
 * https://github.com/carbon-design-system/carbon-components-angular/issues/3519
 */
import { TemplateRef } from '@angular/core';

export interface Node {
  label: string | TemplateRef<any>;
  labelContext?: any;
  value?: any;
  id?: string;
  active?: boolean;
  disabled?: boolean;
  selectable?: boolean;
  expanded?: boolean;
  selected?: boolean;
  icon?: string | TemplateRef<any>;
  iconContext?: any;
  gap?: number;
  children?: Node[];
  [key: string]: any;
}

export interface EventOnNode {
  node: Node;
  event: Event;
}
