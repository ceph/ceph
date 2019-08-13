import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';
import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { OrchestratorService } from '../../../shared/api/orchestrator.service';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { SharedModule } from '../../../shared/shared.module';
import { InventoryComponent } from './inventory.component';

describe('InventoryComponent', () => {
  let component: InventoryComponent;
  let fixture: ComponentFixture<InventoryComponent>;
  let reqHostname: string;

  const inventoryNodes = [
    {
      name: 'host0',
      devices: [
        {
          type: 'hdd',
          id: '/dev/sda'
        }
      ]
    },
    {
      name: 'host1',
      devices: [
        {
          type: 'hdd',
          id: '/dev/sda'
        }
      ]
    }
  ];

  const getIventoryList = (hostname: String) => {
    return hostname ? inventoryNodes.filter((node) => node.name === hostname) : inventoryNodes;
  };

  configureTestBed({
    imports: [SharedModule, HttpClientTestingModule, RouterTestingModule],
    providers: [i18nProviders],
    declarations: [InventoryComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InventoryComponent);
    component = fixture.componentInstance;
    const orchService = TestBed.get(OrchestratorService);
    spyOn(orchService, 'status').and.returnValue(of({ available: true }));
    reqHostname = '';
    spyOn(orchService, 'inventoryList').and.callFake(() => of(getIventoryList(reqHostname)));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have columns that are sortable', () => {
    expect(component.columns.every((column) => Boolean(column.prop))).toBeTruthy();
  });

  it('should return all devices', () => {
    component.getInventory(new CdTableFetchDataContext(() => {}));
    expect(component.devices.length).toBe(2);
  });

  it('should return devices on a host', () => {
    reqHostname = 'host0';
    component.getInventory(new CdTableFetchDataContext(() => {}));
    expect(component.devices.length).toBe(1);
    expect(component.devices[0].hostname).toBe(reqHostname);
  });
});
