import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Location } from '@angular/common';
import { UntypedFormGroup } from '@angular/forms';
import { mergeMap } from 'rxjs/operators';
import { CrudTaskInfo, JsonFormUISchema } from './crud-form.model';
import { Observable } from 'rxjs';
import _ from 'lodash';
import { CdTableSelection } from '../../models/cd-table-selection';

@Component({
  selector: 'cd-crud-form',
  templateUrl: './crud-form.component.html',
  styleUrls: ['./crud-form.component.scss']
})
export class CrudFormComponent implements OnInit {
  model: any = {};
  resource: string;
  task: { message: string; id: string } = { message: '', id: '' };
  form = new UntypedFormGroup({});
  formUISchema$: Observable<JsonFormUISchema>;
  methodType: string;
  urlFormName: string;
  key: string = '';
  selected: CdTableSelection;

  constructor(
    private dataGatewayService: DataGatewayService,
    private activatedRoute: ActivatedRoute,
    private taskWrapper: TaskWrapperService,
    private location: Location,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.activatedRoute.queryParamMap.subscribe((paramMap) => {
      this.formUISchema$ = this.activatedRoute.data.pipe(
        mergeMap((data: any) => {
          this.resource = data.resource || this.resource;
          const url = '/' + this.activatedRoute.snapshot.url.join('/');
          const key = paramMap.get('key') || '';
          return this.dataGatewayService.form(`ui-${this.resource}`, url, key);
        })
      );
      this.formUISchema$.subscribe((data: any) => {
        this.methodType = data.methodType;
        this.model = data.model;
      });
      this.urlFormName = this.router.url.split('/').pop();
      // remove optional arguments
      const paramIndex = this.urlFormName.indexOf('?');
      if (paramIndex > 0) {
        this.urlFormName = this.urlFormName.substring(0, paramIndex);
      }
    });
  }

  async readFileAsText(file: File): Promise<string> {
    let fileReader = new FileReader();
    let text: string = '';
    await new Promise((resolve) => {
      fileReader.onload = (_) => {
        text = fileReader.result.toString();
        resolve(true);
      };
      fileReader.readAsText(file);
    });
    return text;
  }

  async preSubmit(data: { [name: string]: any }) {
    for (const [key, value] of Object.entries(data)) {
      if (value instanceof FileList) {
        let file = value[0];
        let text = await this.readFileAsText(file);
        data[key] = text;
      }
    }
  }

  async submit(data: { [name: string]: any }, taskInfo: CrudTaskInfo) {
    if (data) {
      let taskMetadata = {};
      _.forEach(taskInfo.metadataFields, (field) => {
        taskMetadata[field] = data[field];
      });
      taskMetadata['__message'] = taskInfo.message;
      await this.preSubmit(data);
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask(`crud-component/${this.urlFormName}`, taskMetadata),
          call: this.dataGatewayService.submit(this.resource, data, this.methodType)
        })
        .subscribe({
          complete: () => {
            this.location.back();
          }
        });
    }
  }
}
