import { Directive, Input, TemplateRef, ViewContainerRef } from '@angular/core';

import { AlertPanelComponent } from '../components/alert-panel/alert-panel.component';
import { LoadingPanelComponent } from '../components/loading-panel/loading-panel.component';
import { LoadingStatus } from '../forms/cd-form';

@Directive({
  selector: '[cdFormLoading]'
})
export class FormLoadingDirective {
  constructor(private templateRef: TemplateRef<any>, private viewContainer: ViewContainerRef) {}

  @Input() set cdFormLoading(condition: LoadingStatus) {
    let content: any;

    this.viewContainer.clear();

    switch (condition) {
      case LoadingStatus.Loading:
        content = this.resolveNgContent($localize`Loading form data...`);
        this.viewContainer.createComponent(LoadingPanelComponent, { projectableNodes: content });
        break;
      case LoadingStatus.Ready:
        this.viewContainer.createEmbeddedView(this.templateRef);
        break;
      case LoadingStatus.Error:
        content = this.resolveNgContent($localize`Form data could not be loaded.`);
        const componentRef = this.viewContainer.createComponent(AlertPanelComponent, {
          projectableNodes: content
        });
        (<AlertPanelComponent>componentRef.instance).type = 'error';
        break;
    }
  }

  resolveNgContent(content: string) {
    const element = document.createTextNode(content);
    return [[element]];
  }
}
