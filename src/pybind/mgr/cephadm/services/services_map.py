from typing import TYPE_CHECKING, Type

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator
    from .cephadmservice import CephadmService

registred_services: dict[str, Type["CephadmService"]] = {}
cephadm_services: dict[str, "CephadmService"] = {}

def service_registry_decorator(cls: Type["CephadmService"]) -> None:
    registred_services[str(cls.TYPE)] = cls

def init_services(mgr: "CephadmOrchestrator") -> None:
    for svc in registred_services:
        svc_cls = registred_services[svc]
        cephadm_services[svc] = svc_cls(mgr)
