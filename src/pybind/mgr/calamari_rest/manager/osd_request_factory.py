from calamari_rest.manager.request_factory import RequestFactory
from calamari_rest.types import OsdMap, OSD_IMPLEMENTED_COMMANDS, OSD_FLAGS
from calamari_rest.manager.user_request import OsdMapModifyingRequest, RadosRequest

from rest import global_instance as rest_plugin

class OsdRequestFactory(RequestFactory):
    """
    This class converts CRUD operations to UserRequest objects, and
    exposes non-crud functions to return the appropriate UserRequest.
    """
    def update(self, osd_id, attributes):
        commands = []

        osd_map = rest_plugin().get_sync_object(OsdMap)

        # in/out/down take a vector of strings called 'ids', while 'reweight' takes a single integer

        if 'in' in attributes and bool(attributes['in']) != bool(osd_map.osds_by_id[osd_id]['in']):
            if attributes['in']:
                commands.append(('osd in', {'ids': [attributes['id'].__str__()]}))
            else:
                commands.append(('osd out', {'ids': [attributes['id'].__str__()]}))

        if 'up' in attributes and bool(attributes['up']) != bool(osd_map.osds_by_id[osd_id]['up']):
            if not attributes['up']:
                commands.append(('osd down', {'ids': [attributes['id'].__str__()]}))
            else:
                raise RuntimeError("It is not valid to set a down OSD to be up")

        if 'reweight' in attributes:
            if attributes['reweight'] != float(osd_map.osd_tree_node_by_id[osd_id]['reweight']):
                commands.append(('osd reweight', {'id': osd_id, 'weight': attributes['reweight']}))

        if not commands:
            # Returning None indicates no-op
            return None

        msg_attrs = attributes.copy()
        del msg_attrs['id']

        if msg_attrs.keys() == ['in']:
            message = "Marking osd.{id} {state}".format(
                id=osd_id, state=("in" if msg_attrs['in'] else "out"))
        elif msg_attrs.keys() == ['up']:
            message = "Marking osd.{id} down".format(
                id=osd_id)
        elif msg_attrs.keys() == ['reweight']:
            message = "Re-weighting osd.{id} to {pct}%".format(
                id=osd_id, pct="{0:.1f}".format(msg_attrs['reweight'] * 100.0))
        else:
            message = "Modifying osd.{id} ({attrs})".format(
                id=osd_id, attrs=", ".join(
                    "%s=%s" % (k, v) for k, v in msg_attrs.items()))

        return OsdMapModifyingRequest(message, commands)

    def scrub(self, osd_id):
        return RadosRequest(
            "Initiating scrub on osd.{id}".format(id=osd_id),
            [('osd scrub', {'who': str(osd_id)})])

    def deep_scrub(self, osd_id):
        return RadosRequest(
            "Initiating deep-scrub on osd.{id}".format(id=osd_id),
            [('osd deep-scrub', {'who': str(osd_id)})])

    def repair(self, osd_id):
        return RadosRequest(
            "Initiating repair on osd.{id}".format(id=osd_id),
            [('osd repair', {'who': str(osd_id)})])

    def get_valid_commands(self, osds):
        """
        For each OSD in osds list valid commands
        """
        ret_val = {}
        osd_map = rest_plugin().get_sync_object(OsdMap)
        for osd_id in osds:
            if osd_map.osds_by_id[osd_id]['up']:
                ret_val[osd_id] = {'valid_commands': OSD_IMPLEMENTED_COMMANDS}
            else:
                ret_val[osd_id] = {'valid_commands': []}

        return ret_val

    def _commands_to_set_flags(self, osd_map, attributes):
        commands = []

        flags_not_implemented = set(attributes.keys()) - set(OSD_FLAGS)
        if flags_not_implemented:
            raise RuntimeError("%s not valid to set/unset" % list(flags_not_implemented))

        flags_to_set = set(k for k, v in attributes.iteritems() if v)
        flags_to_unset = set(k for k, v in attributes.iteritems() if not v)
        flags_that_are_set = set(k for k, v in osd_map.flags.iteritems() if v)

        for x in flags_to_set - flags_that_are_set:
            commands.append(('osd set', {'key': x}))

        for x in flags_that_are_set & flags_to_unset:
            commands.append(('osd unset', {'key': x}))

        return commands

    def update_config(self, _, attributes):

        osd_map = rest_plugin().get_sync_object(OsdMap)

        commands = self._commands_to_set_flags(osd_map, attributes)

        if commands:
            return OsdMapModifyingRequest(
                "Modifying OSD config ({attrs})".format(
                    attrs=", ".join("%s=%s" % (k, v) for k, v in attributes.items())
                ), commands)

        else:
            return None
