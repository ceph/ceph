# Controller Model Proposal

## Context and Motivation

Currently the ceph-dashboard controller infrastructure allows to consume HTTP request data through
Python method parameters.

The HTTP request data can be of four kinds:
* Path parameters
* Query parameters
* Body data (JSON encoded)
* HTTP Headers (currently not consumed directly by the controller infrastructure)

The several kinds of request data are binded to the endpoint Python method parameters by matching
the parameter name against the key of the data (with the exception of path parameters, which are
handled a bit different).

This approach works quite well when the data we receive is of the form (key, value) where value is
of some scalar type, i.e., contains a single value and not a complex data structure. But sometimes,
we do receive much more complex data encoded in JSON of variable depth.

For instance, consider the following examle of an HTTP body JSON structure:

```json
{
    "name": "John Doe",
    "contacts": {
        "personal": "96111111",
        "work": "91111111"
    }
}
```

and an endpoint method declared like the following:

```python
@Endpoint('POST')
def create(self, name, contacts):
  # ...
```

In the above example, when the HTTP request is issued, the controller infrastructure will parse the
JSON data and will call the `create` method with the value `John Doe` for parameter `name`, and the
value `{"personal": "96111111", "work": "91111111"}` for parameter `contacts`.
This means that `contacts` variable will actually hold a python dict and there is no information
in the code that can tell us that fact.
Moreover, if we want to automatically generate documentation for our controllers, there is not
enough information either.

Besides the lack of information about the structure of data that is binded to each method parameter,
there is also another feature lacking in the current infrastructure, which is a structured way
of validating the request data.

Many times the data received by our controllers are subject to restrictions of the domain, or even
complex rules. Currently, such validation must be done manually in the endpoint method code, which
leads to a lot of boilerplate code, and most of the times, to be completely inexistent.

The purpose of this proposal is enhance the controller infrastructure with notion of data models
(and please don't associate this notion with database models used to represent relational database
data in an object-oriented way) that allows to specify the structure of the data that is received
by each endpoint and includes simple automatic data validations.

Another important aspect of these models is that it is only intended to be used inside the
controllers. Even if some services might benefit from using the model instances directly it would
create a tight coupling between the service and the controller, which decreases the re-usability
of a service between different controllers.


## Model building blocks

A model will be specified by a class that extends from one of the following base classes:

* `BodyModel`
* `QueryModel`
* `PathModel`
* `HeaderModel`

Then the structure of the data will be specified as class attributes using several helper classes
that extend from the `Attribute` class.

The current list of attribute classes are:

* `String`: for specifying string attributes
* `Int`: for specifying integer attributes
* `ListOf`: for specifying a list of attributes of the same type
* `Model`: for specifying a model class as an attribute

Many other types of attributes can be built, even custom attributes. Each attribute class will
always have at least the following four parameters in the constructor (inherited from the
`Attribute` class):

* `description: str = None`: the description of the attribute (mostly for documentation purposes).
* `validator: Validator = None`: a validator class instance that implements the validation rule.
* `required: bool = True`: whether the attribute is required to have a value or not.
* `label: str = None`: the label to use when serializing the attribute in the transport format.

Validator classes are used to implement validation rules for the attributes of models. Each
validator class will extend from the `Validator` base class.
We already provide a set of standard validator classes, but it will be easy to create custom
validators that can be re-used across the code base.

The current list of validator classes are:

* `Regex`: validates if the attribute value matches regex.
* `Length`: validates if the attribute value length is within some range.
* `NotEmpty`: validates if the attribute value is not empty.
* `IPAddress`: validates if the attribute value is an IP address.
* `Enum`: validates if the attribute value is matches one of the possible options.
* `Gt`: validates if the attribute value is greater than some lower bound.


## How to write a model?

Let's use the NFS-Ganesha controller example to specify a model that describes the requests and
responses structure.

When creating an NFS export using the CephFS backend, the dashboard frontend makes a POST request
to the backend with the following JSON object in the request body.

```json
{
  "cluster_id": "hello",
  "daemons": ["node1", "node2"],
  "fsal":{
    "name": "CEPH",
    "user_id": "fs_a",
    "fs_name": "a",
    "sec_label_xattr": "security.selinux"
  },
  "path": "/mydir",
  "tag": "mytag",
  "pseudo": "/cephfs/mydir",
  "access_type": "RW",
  "squash": "no_root_squash",
  "clients": [{
    "addresses": ["192.168.100.0/24"],
    "access_type": "RO",
    "squash": "no_root_squash"
  },{
    "addresses": ["192.168.1.103", "192.168.1.104"],
    "access_type": "RW",
    "squash": "all_squash"
  }],
  "security_label": true,
  "protocols": [3, 4],
  "transports": ["TCP", "UDP"]
}
```

And the response body is almost similar to the request with exception of the addition of the
`export_id` attribute:

```json
{
  "export_id": 1,
  // ...
}
```

Let's first specify the model for the FSAL object:

```python
from .model import BodyModel, validator as Val, attribute as Attr


class FsalModel(BodyModel):
  name = Attr.String(
    description="Export path",
    validator=Val.Enum("CEPH", "RGW"))

  user_id = Attr.String(
    description="CephX user ID",
    validator=(Val.NotEmpty(), Val.Length(64), ),
    required=False)

  filesystem = Attr.String(
    description="CephFS filesystem ID",
    validator=(Val.NotEmpty(), Val.Length(64), ),
    required=False)

  sec_label_xattr = Attr.String(
    description="Name of xattr for security label",
    validator=(Val.NotEmpty(), Val.Length(64), ),
    required=False)

  rgw_user_id = Attr.String(
    description="RGW user ID",
    validator=(Val.NotEmpty(), Val.Length(64), ),
    required=False)
```

Note that if the attribute class name matches the attribute in the JSON object then there is no
need to use the `label` parameter of the attribute constructor.
The `validator` parameter can be assigned to a validator class instance, or to a tuple of validator
class instances. The latter has the semantics of the logic conjunction.

Now the Client object model:

```python
class ClientModel(BodyModel):
  addresses = Attr.ListOf(
    Attr.String(validator=Val.IPAddress()),
    description="List of IP addresses")

  access_type = Attr.String(
    description="Client access type",
    validator=Val.Enum("RW", "RO", "MDONLY", "MDONLY_RO", "NONE"))

  squash = Attr.String(
    description="Client squash policy",
    validator=Val.Enum("no_root_squash", "root_id_squash", "root_squash", "all_squash"))
```

The `ClientModel` is a bit more interesting because it makes use of the `ListOf` attribute, which
in this case, allows to specify that we have a list of string attributes where each string
attribute must respect the rule of the `IPAddress` validator.

This model also uses the `Enum` validator, which I believe its semantics is self-explanatory.

The nfs-ganesha export model can now be specified as:

```python
class CreateExportModel(BodyModel):
  path = Attr.String(
    description="Export path",
    validator=Val.Regex(r'^/[^><|&()?]*$'))

  cluster_id = Attr.String(
    description="Cluster identifier")

  daemons = Attr.ListOf(
    Attr.String(validator=Val.Length(64)),
    description="List of NFS Ganesha daemons identifiers")

  pseudo = Attr.String(
    description="Pseudo FS path",
    validator=Val.Regex(r'^/[^><|&()?]*$'),
    required=False)

  tag = Attr.String(
    description="NFSv3 export tag",
    validator=Val.Regex(r'^[^/><|:&()]+$'))

  access_type = Attr.String(
    description="Export access type",
    validator=Val.Enum("RW", "RO", "MDONLY", "MDONLY_RO", "NONE"))

  squash = Attr.String(
    description="Export squash policy",
    validator=Val.Enum("no_root_squash", "root_id_squash", "root_squash", "all_squash"))

  security_label = Attr.String(
    description="Security label",
    validator=Val.Length(64))

  protocols = Attr.ListOf(
    Attr.Int(validator=Val.Enum(3, 4)),
    description="List of protocol types")

  transports = Attr.ListOf(
    Attr.String(validator=Val.Enum("TCP", "UDP")),
    description="List of transport types")

  fsal = Attr.Model(
    FsalModel,
    description="FSAL configuration")

  clients = Attr.ListOf(
    Attr.Model(ClientModel),
    description="List of client configurations",
    required=False)

  reload_daemons: Attr.Bool(
    description="Trigger reload of NFS-Ganesha daemons configuration",
    required=False)


class ExportModel(CreateExportModel):
  export_id = Attr.Int(
    description="Export ID",
    validator=Val.Gt(0))
```

Since the attributes of an export are a superset of the attributes to create an export we create
a `CreateExportModel` model class with all attributes of an export except for the `export_id`
attribute and create an `ExportModel` that extends `CreateExportModel` and adds the missing
attribute `export_id`. Extensability of models via class inheritance prevents code duplication.

In the `CreateExportModel` we can see how to include other models as attributes. The `fsal` class
attribute is assigned to the `Model` class attribute instance that receives as the first parameter
of the constructor the class of the model. In this case the `FsalModel`.

We can also compose the `ListOf` attribute with the `Model` attribute as depicted in the `clients`
class attribute.

We could also rewrite the above models in a way that could reduce the code duplication of the
`access_type` and `squash` attributes that are present in both the `ClientModel`, and in the
`CreateExportModel`, by extracting those attributes into their own model class and then extend
from this class.

Using multiple inheritance we can even create a model that is a composition of other models.


## How to use the model class in the controllers?

Currently the NFS-Ganesha controller code for the create method is the following:

```python
@ApiController('/nfs-ganesha/export', Scope.NFS_GANESHA)
class NFSGaneshaExports(RESTController):
  # ...

  @NfsTask('create', {'path': '{path}', 'fsal': '{fsal.name}',
                      'cluster_id': '{cluster_id}'}, 2.0)
  def create(self, path, cluster_id, daemons, pseudo, tag, access_type,
             squash, security_label, protocols, transports, fsal, clients,
             reload_daemons=True):
    if fsal.name not in Ganesha.fsals_available():
      raise NFSException("Cannot create this export. "
                         "FSAL '{}' cannot be managed by the dashboard."
                         .format(fsal.name))
    # ...
    return {
      "export_id": 1,
      #...
    }
```

To use our `CreateExportModel` class the code of the controller would look like the following:

```python
@ApiController('/nfs-ganesha/export', Scope.NFS_GANESHA)
class NFSGaneshaExports(RESTController):
  # ...

  @NfsTask('create', {'path': '{model.path}', 'fsal': '{model.fsal.name}',
                      'cluster_id': '{model.cluster_id}'}, 2.0)
  def create(self, model: CreateExportModel) -> ExportModel:
    if model.fsal.name not in Ganesha.fsals_available():
      raise NFSException("Cannot create this export. "
                         "FSAL '{}' cannot be managed by the dashboard."
                         .format(model.fsal.name))
    # ...
    # build the result model
    return result_model
```

The above code is Python3 only. The controller infrastructure relies on the type annotation to know
which model class to deserialize the JSON body data into. And, the documentation generator will
also rely on the type annotations to generate the documentation.

If we need to support Python2 as well, then we need to declare the model class type in a different
way. The following code example is the proposal for Python2:

```python
@ApiController('/nfs-ganesha/export', Scope.NFS_GANESHA)
class NFSGaneshaExports(RESTController):
  # ...

  @NfsTask('create', {'path': '{model.path}', 'fsal': '{model.fsal.name}',
                      'cluster_id': '{model.cluster_id}'}, 2.0)
  @RequestModel("model", CreateExportModel)
  @ResponseModel(ExportModel)
  def create(self, model):
    # ...
    # build the result model
    return result_model
```

The `RequestModel` decorator receives two parameters, the parameter name of the endpoint method,
and the model class. The `ResponseModel` decorator just requires the model class.

Apart from the model class type declaration differences between Python2 and Python3, the way we use
the model instance inside the endpoint method is the same.
The model instance has an instance attribute for each class attribute initialized with an
`Attribute` subclass with the same name.

To manually instantiate a model we can just call the model class constructor with the values for
each attribute, or just for a subset of the attributes:

```python
export = ExportModel(
  export_id=1,
  path="/mypath",
  cluster_id="hello",
  daemons=["node1", "node2"],
  pseudo="/cephfs/mypath",
  tag="mytag",
  access_type="RW",
  squash="no_root_squash",
  security_label=True,
  protocols=[3, 4],
  transports=["TCP", "UDP"])

export.fsal = FsalModel(
  name="CEPH",
  user_id="fs_a",
  fs_name="a",
  sec_label_xattr="security.selinux")

export.clients.append(ClientModel(
  addresses=["192.168.100.0/24"],
  access_type="RO",
  squash="no_root_squash"
))

export.clients.append(ClientModel(
  addresses=["192.168.1.103", "192.168.1.104"],
  access_type="RW",
  squash="all_squash"
))
```

Model validations are triggered automatically by the controller infrastructure before calling the
endpoint method. This way when the endpoint method is called the model was already validated, which
can reduce a lot of validation code in the controller.
Any validation error found in a model will originate an HTTP 400 (Bad Request) response.


## Models for path, query, and header data

### Path parameters

We can also use model classes to represent path parameters. Path parameters correspond to data
passed in the URL of the request and is usually used as the key of the resources.

Currently, path parameters are encoded by our controllers as python method parameters following
some rules as specified in the `HACKING.rst` file. One of the main limitations of current approach
is that the ordering or position of the method parameters in the endpoint methods matters to
distinguish path parameters from body or query parameters.

By using a model class extended by `PathModel`, this limitation disappears because the path
parameters become explicit in the endpoint method declaration, and the position of the parameter
does not matter anymore. Moreover, we also gain the automatic validation of the path parameter
value, as well as, the correct encoding of the path value string into the respective type.

Let's continue with the nfs-ganesha example to show how to use path models.

Currently the nfs-ganesha controller code is the following:

```python
@ApiController('/nfs-ganesha/export', Scope.NFS_GANESHA)
class NFSGaneshaExports(RESTController):
  RESOURCE_ID = "cluster_id/export_id"
  # ...

  @NfsTask('edit', {'cluster_id': '{cluster_id}', 'export_id': '{export_id}', 2.0)
  def set(self, cluster_id, export_id, path, daemons, pseudo, tag, access_type,
          squash, security_label, protocols, transports, fsal, clients,
          reload_daemons=True):
    export_id = int(export_id)
    ganesha_conf = GaneshaConf.instance(cluster_id)
    # ...
    return {
      "export_id": 1,
      #...
    }
```

As you can see from above the two first parameters of the `set` method correspond to the path
parameters declared in the class attribute `RESOURCE_ID`. These parameters must appear before the
parameters used for the body data in the endpoint method declaration.

With the new path models we can rewrite the above example as the following:

```python
class ExportKeyModel(PathModel):
  cluster_id = Attr.String(
    description="Cluster identifier")

   export_id = Attr.Int(
    description="Export ID",
    validator=Val.Gt(0))

@ApiController('/nfs-ganesha/export', Scope.NFS_GANESHA)
class NFSGaneshaExports(RESTController):
  # ...

  @NfsTask('edit', {'cluster_id': '{key.cluster_id}', 'export_id': '{key.export_id}', 2.0)
  def set(self, key: ExportKeyModel, export: ExportModel) -> ExportModel:
    # export_id = int(key.export_id)  no need to do this, key.export_id is already an interger
    ganesha_conf = GaneshaConf.instance(key.cluster_id)
    # ...
    # build the result model
    return result_model
```

Since now the parameter declaration ordering does not matter we could also declare the `set` method
as:

```python
# ...
def set(self, export: ExportModel, key: ExportKeyModel) -> ExportModel:
# ...
```

One important note in the above example is that we removed the `RESOURCE_ID` declaration because
we now use the structure of the `ExportKeyModel` class to understand how to parse the path parameter
values from the request URL. This also means that the declaration order of the attributes in the
path model class matters. In this case `cluster_id` comes before `export_id` and therefore, if
the request URL is of the form `https://dashboard.dom/api/nfs-ganesha/export/hello/1` then
`cluster_id` will have the `hello` value and `export_id` will have the `1` value.


### Query parameters

Query parameters are parsed from the request URL and are encoded as python method parameters.

Usually these method parameters are declared as optional parameters in the endpoints methods, as
most of the times query parameters are not mandatory.

To encode query parameters as a model, we just need to create a subclass of the `QueryModel` class.

I will not add an example in this section as the class structure for the query model, and its usage
in a controller is similar to the examples in previous sections.


### Header parameters

An HTTP request also transports data in the form of HTTP headers. Currently our controller
infrastructure does not support the retrieval of this kind of information directly.
To access the headers we still need to go through the cherrypy library, which breaks the controller
infrastructure abstraction.

To fix this problem we can get the headers data the same way as we get other types of data of an
HTTP request, using a model class.

Let's use an example on how to use the headers model:

```python
class SomeHeadersModel(HeaderModel):
  content_type = Attr.String(
    validator=Val.Enum("application/json"),
    label="content-type")

  host = Attr.String()
```

When declaring a parameter in an endpoint method of the type of the above class `SomeHeadersModel`,
the header model instance will be populated with the `Content-Type` and `Host` headers values.

An important aspect of the above class is that we are explicitly restricting the value of the
`Content-Type` header to only allow JSON content. This is an interesting feature that we gain from
the usage of models to access the request headers.

Here's another example:

```python
class PaginationRequestModel(HeaderModel):
  page_num = Attr.Int(
    description="Page number",
    validator=Val.Gte(0),
    label="x-page-num")

  page_limit = Attr.Int(
    description="Maximum number of entries per page",
    validator=Val.Gte(0),
    label="x-page-limit")


class PaginationResponseModel(PaginationRequestModel):
  page_total = Attr.Int(
    description="Total number of pages available",
    label="x-page-total"
  )
```

Adding the support for pagination on an endpoint using just headers is as simply as creating a
header model class like the above and declare the respective parameter in the endpoint method.

Another interesting thing is the `PaginationResponseModel` that is intended to be use as a way to
send custom header values in the HTTP response. We still haven't saw an example in this document on
how that could be done. In the next section we will discuss how can we return both body and header
models in an endpoint method.


## Returning body and header models

From the examples we've seen so far, endpoint methods only allow to return one model, the body
model.

To return a body model instance and a header model instance, we must use a special class called
`Response`. The `Response` class has the following constructor definition:

```python
class Response(object):
  def __init__(self, status_code: int=None, body: BodyModel=None, header: HeaderModel=None):
    # ...
```

The `Response` class, not only allows you to return both body and header data from an endpoint
method, but also specify the status code of the HTTP response if you need to return a different
status code from the default.

To use the `Response` class we just need to return an instance of the class as the return value of
an endpoint method, and the controller infrastructure will take care of build the HTTP response
accordingly.

Example:

```python
def list(self) -> Response:
  # build body model
  # build header model
  return Response(header=header, body=body)
```


