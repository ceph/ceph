class ZoneEPs:
    def __init__(self):
        self.endpoints = set()

    def add(self, ep):
        if not ep:
            return

        self.endpoints.add(ep)

    def diff(self, zep):
        return list(self.endpoints.difference(zep.endpoints))

    def get_all(self):
        for ep in self.endpoints:
            yield ep


class RealmEPs:
    def __init__(self):
        self.zones = {}

    def add(self, zone, ep=None):
        if not zone:
            return

        z = self.zones.get(zone)
        if not z:
            z = ZoneEPs()
            self.zones[zone] = z

        z.add(ep)

    def diff(self, rep):
        result = {}
        for z, zep in rep.zones.items():
            myzep = self.zones.get(z)
            if not myzep:
                continue

            d = myzep.diff(zep)
            if len(d) > 0:
                result[z] = myzep.diff(zep)

        return result

    def get_all(self):
        for z, zep in self.zones.items():
            eps = []
            for ep in zep.get_all():
                eps.append(ep)
            yield z, eps


class RealmsEPs:
    def __init__(self):
        self.realms = {}

    def add(self, realm, zone=None, ep=None):
        if not realm:
            return

        r = self.realms.get(realm)
        if not r:
            r = RealmEPs()
            self.realms[realm] = r

        r.add(zone, ep)

    def diff(self, rep):
        result = {}

        for r, rep in rep.realms.items():
            myrealm = self.realms.get(r)
            if not myrealm:
                continue

            d = myrealm.diff(rep)
            if d:
                result[r] = d

        return result

    def get_all(self):
        result = {}
        for r, rep in self.realms.items():
            zs = {}
            for z, eps in rep.get_all():
                zs[z] = eps

            result[r] = zs

        return result
