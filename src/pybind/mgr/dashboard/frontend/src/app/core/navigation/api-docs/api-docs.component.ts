import { Component, OnInit, ViewEncapsulation } from '@angular/core';

import SwaggerUI from 'swagger-ui-dist/swagger-ui-bundle';

@Component({
  selector: 'cd-api-docs',
  templateUrl: './api-docs.component.html',
  styleUrls: ['./api-docs.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class ApiDocsComponent implements OnInit {
  ngOnInit(): void {
    SwaggerUI({
      url: window.location.origin + '/docs/openapi.json',
      dom_id: '#swagger-ui',
      layout: 'BaseLayout'
    });
  }
}
