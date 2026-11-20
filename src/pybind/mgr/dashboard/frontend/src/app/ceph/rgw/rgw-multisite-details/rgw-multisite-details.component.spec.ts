import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { SharedModule } from '~/app/shared/shared.module';

import { RgwMultisiteDetailsComponent } from './rgw-multisite-details.component';
import { RouterTestingModule } from '@angular/router/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NgbNavModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwMultisiteRealmFormComponent } from '../rgw-multisite-realm-form/rgw-multisite-realm-form.component';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission, Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

describe('RgwMultisiteDetailsComponent', () => {
  let component: RgwMultisiteDetailsComponent;
  let fixture: ComponentFixture<RgwMultisiteDetailsComponent>;
  let debugElement: DebugElement;
  let modalService: ModalCdsService;

  configureTestBed({
    declarations: [RgwMultisiteDetailsComponent],
    imports: [HttpClientTestingModule, SharedModule, RouterTestingModule, NgbNavModule],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteDetailsComponent);
    component = fixture.componentInstance;
    debugElement = fixture.debugElement;
    modalService = TestBed.inject(ModalCdsService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display right title', () => {
    const span = debugElement.nativeElement.querySelector('.card-header');
    expect(span.textContent.trim()).toBe('Topology Viewer');
  });

  it('should call openModal to open realm modal', () => {
    const entity = { data: { type: 'realm' } };
    const edit = true;

    spyOn(modalService, 'show');

    component.openModal(entity, edit);

    expect(modalService.show).toHaveBeenCalledWith(RgwMultisiteRealmFormComponent, {
      resource: 'realm',
      action: component.actionLabels.EDIT,
      info: entity,
      defaultsInfo: component.defaultsInfo,
      multisiteInfo: component.multisiteInfo
    });
  });

  describe('abstractTreeData active node refresh', () => {
    const realm = { id: 'realm-1', name: 'realm1' };
    const zoneInZonegroup = {
      id: 'zone-1',
      name: 'zone1',
      endpoints: ['http://a:80'],
      tier_type: 'cloud'
    };
    const zonegroup = {
      id: 'zg-1',
      name: 'zg1',
      realm_id: 'realm-1',
      is_master: true,
      master_zone: 'zone-1',
      endpoints: ['http://a:80'],
      placement_targets: [],
      default_placement: 'default-placement',
      zones: [zoneInZonegroup]
    };
    const zone = {
      id: 'zone-1',
      name: 'zone1',
      endpoints: ['http://a:80'],
      system_key: { access_key: 'ak', secret_key: 'sk' }
    };

    const buildInfo = (overrides: { realms?: any[]; zonegroups?: any[]; zones?: any[] } = {}) =>
      [
        { realms: overrides.realms ?? [realm], default_realm: 'realm-1' },
        { zonegroups: overrides.zonegroups ?? [zonegroup], default_zonegroup: 'zg-1' },
        { zones: overrides.zones ?? [zone], default_zone: 'zone-1' }
      ] as [object, object, object];

    const refresh = (info: [object, object, object]) => component['abstractTreeData'](info);

    it('should not touch metadata when there is no active node', () => {
      component.metadata = { foo: 'bar' };
      component.activeNodeId = undefined;

      refresh(buildInfo());

      expect(component.metadata).toEqual({ foo: 'bar' });
      expect(component.metadataTitle).toBeUndefined();
    });

    it('should refresh metadata and title of an active realm', () => {
      component.activeNodeId = 'realm-1';
      component.metadata = { id: 'realm-1', name: 'old-name' };

      refresh(buildInfo());

      expect(component.metadata).toEqual(realm);
      expect(component.metadataTitle).toBe('realm1');
      expect(component.activeNodeId).toBe('realm-1');
    });

    it('should refresh metadata and title of an active zonegroup', () => {
      component.activeNodeId = 'zg-1';
      component.metadata = { id: 'zg-1', name: 'old-name' };

      refresh(buildInfo());

      expect(component.metadata).toEqual(zonegroup);
      expect(component.metadataTitle).toBe('zg1');
    });

    it('should prefer the zonegroup zone entry for an active zone', () => {
      component.activeNodeId = 'zone-1';
      component.metadata = undefined;

      refresh(buildInfo());

      expect(component.metadata).toEqual(zoneInZonegroup);
      expect(component.metadataTitle).toBe('zone1');
    });

    it('should fall back to the standalone zone when no zonegroup lists it', () => {
      component.activeNodeId = 'zone-1';

      refresh(buildInfo({ realms: [], zonegroups: [], zones: [zone] }));

      expect(component.metadata).toEqual(zone);
      expect(component.metadataTitle).toBe('zone1');
    });

    it('should keep the same metadata reference when the content is unchanged', () => {
      component.activeNodeId = 'zg-1';
      const existing = { ...zonegroup, zones: [{ ...zoneInZonegroup }] };
      component.metadata = existing;

      refresh(buildInfo());

      expect(component.metadata).toBe(existing);
    });

    it('should replace metadata when the entity was updated', () => {
      component.activeNodeId = 'zg-1';
      component.metadata = zonegroup;
      const updated = { ...zonegroup, endpoints: ['http://b:80'] };

      refresh(buildInfo({ zonegroups: [updated] }));

      expect(component.metadata).toEqual(updated);
      expect(component.metadata).not.toBe(zonegroup);
    });

    it('should clear metadata and active node when the entity was deleted', () => {
      component.activeNodeId = 'realm-1';
      component.metadata = realm;
      component.metadataTitle = 'realm1';

      refresh(buildInfo({ realms: [] }));

      expect(component.metadata).toBeUndefined();
      expect(component.metadataTitle).toBeUndefined();
      expect(component.activeNodeId).toBeUndefined();
    });
  });

  describe('disableActions', () => {
    it('should return true when rgw.create is false', () => {
      component.permissions = new Permissions({ rgw: ['read'] });
      expect(component.disableActions).toBe(true);
    });

    it('should return false when rgw.create is true', () => {
      component.permissions = new Permissions({ rgw: ['read', 'create'] });
      expect(component.disableActions).toBe(false);
    });
  });

  describe('multisite create table actions', () => {
    const createFixture = (rgwPermissions: string[]) => {
      spyOn(TestBed.inject(AuthStorageService), 'getPermissions').and.returnValue(
        new Permissions({ rgw: rgwPermissions, 'config-opt': ['read'] })
      );
      const testFixture = TestBed.createComponent(RgwMultisiteDetailsComponent);
      testFixture.detectChanges();
      return testFixture;
    };

    const getMultisiteTableActions = (
      testFixture: ComponentFixture<RgwMultisiteDetailsComponent>
    ): TableActionsComponent =>
      testFixture.debugElement.query(By.css('cd-table-actions.multisite-actions'))
        .componentInstance;

    it('should pass disabled=true to TableActionsComponent when rgw.create is false', () => {
      const testFixture = createFixture(['read']);
      const tableActions = getMultisiteTableActions(testFixture);
      const button = testFixture.debugElement.query(
        By.css('cd-table-actions.multisite-actions button.cds--btn--tertiary')
      );

      expect(testFixture.componentInstance.disableActions).toBe(true);
      expect(tableActions.disabled).toBe(true);
      expect(button.nativeElement.disabled).toBe(true);
    });

    it('should pass disabled=false to TableActionsComponent when rgw.create is true', () => {
      const testFixture = createFixture(['read', 'create']);
      const tableActions = getMultisiteTableActions(testFixture);
      const button = testFixture.debugElement.query(
        By.css('cd-table-actions.multisite-actions button.cds--btn--tertiary')
      );

      expect(testFixture.componentInstance.disableActions).toBe(false);
      expect(tableActions.disabled).toBe(false);
      expect(button.nativeElement.disabled).toBe(false);
    });
  });

  describe('TableActionsComponent disabled input', () => {
    const createTableActionsFixture = (disabled: boolean) => {
      const tableActionsFixture = TestBed.createComponent(TableActionsComponent);
      const tableActionsComponent = tableActionsFixture.componentInstance;
      tableActionsComponent.permission = new Permission(['create']);
      tableActionsComponent.dropDownOnly = 'Actions';
      tableActionsComponent.dropDownOnlyBtnColor = 'tertiary';
      tableActionsComponent.tableActions = [
        { permission: 'create', icon: Icons.add, name: 'Create Realm', click: () => undefined }
      ];
      tableActionsComponent.selection = new CdTableSelection();
      tableActionsComponent.disabled = disabled;
      tableActionsFixture.detectChanges();
      return tableActionsFixture;
    };

    const getDropdownTriggerButton = (
      tableActionsFixture: ComponentFixture<TableActionsComponent>
    ) => tableActionsFixture.debugElement.query(By.css('button.cds--btn--tertiary'));

    it('should disable the trigger button when disabled is true', () => {
      const tableActionsFixture = createTableActionsFixture(true);
      const button = getDropdownTriggerButton(tableActionsFixture);

      expect(tableActionsFixture.componentInstance.disabled).toBe(true);
      expect(button.nativeElement.disabled).toBe(true);
    });

    it('should enable the trigger button when disabled is false', () => {
      const tableActionsFixture = createTableActionsFixture(false);
      const button = getDropdownTriggerButton(tableActionsFixture);

      expect(tableActionsFixture.componentInstance.disabled).toBe(false);
      expect(button.nativeElement.disabled).toBe(false);
    });
  });
});
