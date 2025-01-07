import { Component, OnInit } from '@angular/core';

import SwaggerUI from 'swagger-ui';

@Component({
  selector: 'cd-api-docs',
  templateUrl: './api-docs.component.html',
  styleUrls: ['./api-docs.component.scss']
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
