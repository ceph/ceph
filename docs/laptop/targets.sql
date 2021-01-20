begin transaction;
insert into nodes (name, machine_type, is_vm, locked, up) values ('localhost', 'libvirt', false, true, true);
insert into nodes (name, machine_type, is_vm, locked, up, mac_address, vm_host_id) values
('target-00.local', 'vps', true, false, false, '52:54:00:00:00:00', (select id from nodes where name='localhost')),
('target-01.local', 'vps', true, false, false, '52:54:00:00:00:01', (select id from nodes where name='localhost')),
('target-02.local', 'vps', true, false, false, '52:54:00:00:00:02', (select id from nodes where name='localhost')),
('target-03.local', 'vps', true, false, false, '52:54:00:00:00:03', (select id from nodes where name='localhost'));
commit transaction

