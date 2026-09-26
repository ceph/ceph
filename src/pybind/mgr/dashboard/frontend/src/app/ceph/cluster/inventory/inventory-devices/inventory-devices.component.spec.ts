import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { OrchestratorFeature } from '~/app/shared/models/orchestrator.enum';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { InventoryDevice } from './inventory-device.model';
import { InventoryDevicesComponent } from './inventory-devices.component';

describe('InventoryDevicesComponent', () => {
  let component: InventoryDevicesComponent;
  let fixture: ComponentFixture<InventoryDevicesComponent>;
  let orchService: OrchestratorService;
  let hostService: HostService;

  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({ osd: ['read', 'update', 'create', 'delete'] });
    }
  };

  const mockOrchStatus = (available: boolean, features?: OrchestratorFeature[]) => {
    const orchStatus: OrchestratorStatus = { available: available, message: '', features: {} };
    if (features) {
      features.forEach((feature: OrchestratorFeature) => {
        orchStatus.features[feature] = { available: true };
      });
    }
    component.orchStatus = orchStatus;
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      FormsModule,
      HttpClientTestingModule,
      SharedModule,
      RouterTestingModule
    ],
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      TableActionsComponent
    ],
    declarations: [InventoryDevicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InventoryDevicesComponent);
    component = fixture.componentInstance;
    hostService = TestBed.inject(HostService);
    orchService = TestBed.inject(OrchestratorService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have columns that are sortable', () => {
    expect(component.columns.every((column) => Boolean(column.prop))).toBeTruthy();
  });

  it('should call inventoryDataList only when showOnlyAvailableData is true', () => {
    const hostServiceSpy = spyOn(hostService, 'inventoryDeviceList').and.callThrough();
    component.getDevices();
    expect(hostServiceSpy).toBeCalledTimes(0);
    component.showAvailDeviceOnly = true;
    component.getDevices();
    expect(hostServiceSpy).toBeCalledTimes(1);
  });

  describe('table actions', () => {
    const fakeDevices = require('./fixtures/inventory_list_response.json');

    beforeEach(() => {
      component.devices = fakeDevices;
      component.selectionType = 'single';
      fixture.detectChanges();
    });

    const verifyTableActions = async (
      tableActions: CdTableAction[],
      expectResult: {
        [action: string]: { disabled: boolean; disableDesc: string };
      }
    ) => {
      const component = fixture.componentInstance;
      const selection = component.selection;
      const tableActionElement = fixture.debugElement.query(By.directive(TableActionsComponent));
      const tableActionComponent: TableActionsComponent = tableActionElement.componentInstance;
      tableActionComponent.selection = selection;

      const actions = {};
      tableActions.forEach((action) => {
        if (expectResult[action.name]) {
          actions[action.name] = {
            disabled: tableActionComponent.disableSelectionAction(action),
            disableDesc: tableActionComponent.useDisableDesc(action) || ''
          };
        }
      });
      expect(actions).toEqual(expectResult);
    };

    const testTableActions = async (
      orch: boolean,
      features: OrchestratorFeature[],
      tests: { selectRow?: number; expectResults: any }[]
    ) => {
      mockOrchStatus(orch, features);
      fixture.detectChanges();
      await fixture.whenStable();

      for (const test of tests) {
        if (test.selectRow) {
          component.selection = new CdTableSelection();
          component.selection.selected = [test.selectRow];
        }
        await verifyTableActions(component.tableActions, test.expectResults);
      }
    };

    it('should have correct states when Orchestrator is enabled', async () => {
      const tests = [
        {
          expectResults: {
            Identify: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeDevices[0],
          expectResults: {
            Identify: { disabled: false, disableDesc: '' }
          }
        }
      ];

      const features = [OrchestratorFeature.DEVICE_BLINK_LIGHT];
      await testTableActions(true, features, tests);
    });

    it('should have correct states when Orchestrator is disabled', async () => {
      const resultNoOrchestrator = {
        disabled: true,
        disableDesc: orchService.disableMessages.noOrchestrator
      };
      const tests = [
        {
          expectResults: {
            Identify: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeDevices[0],
          expectResults: {
            Identify: resultNoOrchestrator
          }
        }
      ];
      await testTableActions(false, [], tests);
    });

    it('should have correct states when Orchestrator features are missing', async () => {
      const resultMissingFeatures = {
        disabled: true,
        disableDesc: orchService.disableMessages.missingFeature
      };
      const expectResults = [
        {
          expectResults: {
            Identify: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeDevices[0],
          expectResults: {
            Identify: resultMissingFeatures
          }
        }
      ];
      await testTableActions(true, [], expectResults);
    });
  });

  describe('search', () => {
    const mockDevices: InventoryDevice[] = [
      {
        hostname: 'node-01',
        uid: 'uid-1',
        path: '/dev/sda',
        sys_api: {
          vendor: 'Samsung',
          model: 'SSD 980 PRO',
          size: 1000,
          rotational: '0',
          human_readable_size: '1 TB'
        },
        available: true,
        rejected_reasons: [],
        device_id: 'samsung-1',
        human_readable_type: 'ssd',
        osd_ids: [123]
      },
      {
        hostname: 'node-01',
        uid: 'uid-2',
        path: '/dev/sdb',
        sys_api: {
          vendor: 'Seagate',
          model: 'ST2000NM',
          size: 2000,
          rotational: '1',
          human_readable_size: '2 TB'
        },
        available: true,
        rejected_reasons: [],
        device_id: 'seagate-1',
        human_readable_type: 'hdd',
        osd_ids: [456]
      },
      {
        hostname: 'node-02',
        uid: 'uid-3',
        path: '/dev/nvme0n1',
        sys_api: {
          vendor: 'Intel',
          model: 'DC P4510',
          size: 4000,
          rotational: '0',
          human_readable_size: '4 TB'
        },
        available: false,
        rejected_reasons: ['locked'],
        device_id: 'intel-1',
        human_readable_type: 'ssd',
        osd_ids: [123, 789]
      }
    ];

    const search = (term: string): InventoryDevice[] => {
      const table = component.table;
      table.search = term;
      table.updateFilter();
      return table.rows;
    };

    beforeEach(() => {
      component.searchField = true;
      component.devices = mockDevices;
      fixture.detectChanges();
    });

    it('should render the Carbon search field with an accessible label', () => {
      const searchField = fixture.debugElement.query(By.css('cds-table-toolbar-search'));
      expect(searchField).toBeTruthy();
      expect(searchField.componentInstance.placeholder).toBe('Search physical disks');
      expect(searchField.componentInstance.label).toBe('Search physical disks');
      expect(searchField.componentInstance.ariaLabel).toBe('Search physical disks');
    });

    it('should search by hostname', () => {
      const rows = search('node-01');
      expect(rows.length).toBe(2);
      expect(rows.every((row) => row.hostname === 'node-01')).toBe(true);
    });

    it('should search by device path', () => {
      const rows = search('/dev/sda');
      expect(rows.length).toBe(1);
      expect(rows[0].path).toBe('/dev/sda');
    });

    it('should search by vendor', () => {
      const rows = search('Samsung');
      expect(rows.length).toBe(1);
      expect(rows[0].sys_api.vendor).toBe('Samsung');
    });

    it('should search by model', () => {
      const rows = search('DC P4510');
      expect(rows.length).toBe(1);
      expect(rows[0].sys_api.model).toBe('DC P4510');
    });

    it('should search by OSD ID', () => {
      const rows = search('456');
      expect(rows.length).toBe(1);
      expect(rows[0].osd_ids).toContain(456);
    });

    it('should search by displayed OSD ID', () => {
      const rows = search('osd.456');
      expect(rows.length).toBe(1);
      expect(rows[0].osd_ids).toContain(456);
    });

    it('should search OSD IDs using the osd. prefix', () => {
      const rows = search('osd.');
      expect(rows.length).toBe(mockDevices.length);
      expect(rows.every((row) => row.osd_ids.length > 0)).toBe(true);
    });

    it('should search by disk type', () => {
      const rows = search('ssd');
      expect(rows.length).toBe(2);
      expect(rows.every((row) => row.human_readable_type === 'ssd')).toBe(true);
    });

    it('should match search terms case-insensitively', () => {
      expect(search('SAMSUNG').length).toBe(1);
      expect(search('Node-01').length).toBe(2);
      expect(search('SSD').length).toBe(2);
    });

    it('should support partial matches', () => {
      expect(
        search('node-')
          .map((row) => row.hostname)
          .sort()
      ).toEqual(['node-01', 'node-01', 'node-02']);
      expect(search('/dev/sd').length).toBe(2);
      expect(search('980').length).toBe(1);
    });

    it('should restore the complete list when the search is cleared', () => {
      expect(search('Intel').length).toBe(1);
      component.table.onClearSearch();
      expect(component.table.search).toBe('');
      expect(component.table.rows.length).toBe(mockDevices.length);
    });

    it('should show no matching results when the search term matches nothing', () => {
      const rows = search('no-such-disk');
      expect(rows.length).toBe(0);
      expect(component.table.displayedEmptyStateTitle).toBe('No matching physical disks');
      expect(component.table.displayedEmptyStateMessage).toBe(
        'No physical disks match the current search criteria.'
      );
    });

    it('should filter client-side without additional API requests', () => {
      const hostServiceSpy = spyOn(hostService, 'inventoryDeviceList');
      search('Samsung');
      expect(hostServiceSpy).not.toHaveBeenCalled();
    });

    it('should apply search on top of an existing column filter', () => {
      const table = component.table;
      table.initColumnFilters();
      table.updateColumnFilterOptions();
      const hostnameFilter = table.columnFilters.find(
        (filter) => filter.column.prop === 'hostname'
      );
      expect(hostnameFilter).toBeTruthy();
      table.onChangeFilter('node-01', hostnameFilter);
      table.onSubmitFilter();
      expect(table.rows.length).toBe(2);

      table.search = 'ssd';
      table.updateFilter();
      expect(table.rows.length).toBe(1);
      expect(table.rows[0].hostname).toBe('node-01');
      expect(table.rows[0].human_readable_type).toBe('ssd');
    });

    it('should keep the filtered result set when sorting', () => {
      const table = component.table;
      table.search = 'ssd';
      table.updateFilter();
      expect(table.rows.length).toBe(2);

      table.doSorting(0);
      expect(table.rows.length).toBe(2);
      expect(table.rows.every((row) => row.human_readable_type === 'ssd')).toBe(true);
    });

    it('should keep search results available for pagination', () => {
      const table = component.table;
      table.search = 'ssd';
      table.updateFilter();
      expect(table.rows.length).toBe(2);
      expect(table.model.totalDataLength).toBe(2);

      table.doPagination({ page: 1, size: 1, filteredData: table.rows });
      expect(table.rows.length).toBe(2);
      expect(table.model.totalDataLength).toBe(2);
    });
  });

  it('should hide the search field by default', () => {
    fixture.detectChanges();
    expect(component.searchField).toBe(false);
    expect(fixture.debugElement.query(By.css('cds-table-toolbar-search'))).toBeFalsy();
  });
});
