import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { DataGatewayService } from '~/app/shared/services/data-gateway.service';
import { BackButtonComponent } from '~/app/shared/components/back-button/back-button.component';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Location } from '@angular/common';

@Component({
  selector: 'cd-crud-form',
  templateUrl: './crud-form.component.html',
  styleUrls: ['./crud-form.component.scss']
})
export class CrudFormComponent implements OnInit {
  uiSchema: any;
  controlSchema: any;
  data: any;
  widgets: any = {
    cancel: BackButtonComponent
  };
  resource: string;
  title: string;

  formOptions = {
    defautWidgetOptions: {
      validationMessages: {
        required: 'This field is required'
      }
    }
  };
  constructor(
    private dataGatewayService: DataGatewayService,
    private activatedRoute: ActivatedRoute,
    private taskWrapper: TaskWrapperService,
    private location: Location
  ) {}

  ngOnInit(): void {
    this.activatedRoute.data.subscribe((data: any) => {
      this.resource = data.resource;
      this.dataGatewayService.list(`ui-${this.resource}`).subscribe((response: any) => {
        this.title = response.forms[0].control_schema.title;
        this.uiSchema = response.forms[0].ui_schema;
        this.controlSchema = response.forms[0].control_schema;
      });
    });
  }

  submit(data: any) {
    if (data) {
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask('ceph-user/create', {
            user_entity: data.user_entity
          }),
          call: this.dataGatewayService.create(this.resource, data)
        })
        .subscribe({
          complete: () => {
            this.location.back();
          }
        });
    }
  }
}
