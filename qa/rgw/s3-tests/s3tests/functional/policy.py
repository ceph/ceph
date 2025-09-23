import json

class Statement(object):
    def __init__(self, action, resource, principal = {"AWS" : "*"}, effect= "Allow", condition = None):
        self.principal = principal
        self.action = action
        self.resource = resource
        self.condition = condition
        self.effect = effect

    def to_dict(self):
        d = { "Action" : self.action,
              "Principal" : self.principal,
              "Effect" : self.effect,
              "Resource" : self.resource
        }

        if self.condition is not None:
            d["Condition"] = self.condition

        return d

class Policy(object):
    def __init__(self):
        self.statements = []

    def add_statement(self, s):
        self.statements.append(s)
        return self

    def to_json(self):
        policy_dict = {
            "Version" : "2012-10-17",
            "Statement":
            [s.to_dict() for s in self.statements]
        }

        return json.dumps(policy_dict)

def make_json_policy(action, resource, principal={"AWS": "*"}, conditions=None):
    """
    Helper function to make single statement policies
    """
    s = Statement(action, resource, principal, condition=conditions)
    p = Policy()
    return p.add_statement(s).to_json()
