import { configureTestBed } from '~/testing/unit-test-helper';
/* tslint:disable:no-unused-variable */

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject } from '@angular/core/testing';

import { DataGatewayService } from './data-gateway.service';
import { RouterTestingModule } from '@angular/router/testing';

describe('Service: DataGateway', () => {
  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule],
    providers: [DataGatewayService]
  });

  it('should ...', inject([DataGatewayService], (service: DataGatewayService) => {
    expect(service).toBeTruthy();
  }));
});
