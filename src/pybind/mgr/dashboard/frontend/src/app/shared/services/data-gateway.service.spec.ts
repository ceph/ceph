/* tslint:disable:no-unused-variable */

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { DataGatewayService } from './data-gateway.service';

describe('Service: DataGateway', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [DataGatewayService]
    });
  });

  it('should ...', inject([DataGatewayService], (service: DataGatewayService) => {
    expect(service).toBeTruthy();
  }));
});
