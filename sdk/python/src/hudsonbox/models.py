# Generated from api/openapi.json. Do not edit.
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
from ._wire import Model, _Omitted, _OMITTED

@dataclass(kw_only=True, repr=False)
class RequestedResources(Model):
    vcpu: int
    memory_mib: int
    disk_mib: int

@dataclass(kw_only=True, repr=False)
class CreateRequest(Model):
    image_digest: str
    name: str | None = None
    resources: RequestedResources
    correlation_id: str | None = None

@dataclass(kw_only=True, repr=False)
class CommandInput(Model):
    argv: list[str]
    env: dict[str, str] = field(default_factory=dict)
    cwd: str = '/'
    deadline_unix_ms: int
    output_limit: int = 1048576
    expected_generation: int | _Omitted = _OMITTED

@dataclass(kw_only=True, repr=False)
class DestroyRequest(Model):
    correlation_id: str | None = None

@dataclass(kw_only=True, repr=False)
class CancelRequest(Model):
    pass

@dataclass(kw_only=True, repr=False)
class AdmittedResponse(Model):
    sandbox_id: str
    operation_id: str
    status: str
    status_url: str

@dataclass(kw_only=True, repr=False)
class ProblemBody(Model):
    operation_id: str | None = None
    title: str
    status: int
    code: str

@dataclass(kw_only=True, repr=False)
class OperationBody(Model):
    response_expired: bool = False
    operation_id: str
    sandbox_id: str
    target_operation_id: str | None = None
    kind: str
    status: str
    phase: str | None = None
    output_status: str | None = None
    result: Any | None = None
    error: Any | None = None
    created_at: str
    completed_at: str | None = None

@dataclass(kw_only=True, repr=False)
class SandboxBody(Model):
    observation_simulated: bool | None = None
    sandbox_id: str
    name: str | None = None
    desired_state: str
    observed_state: str
    observed_at: str | None = None
    image_digest: str
    resources: Any
    generation: int
    active_operation_id: str | None = None
    created_at: str

@dataclass(kw_only=True, repr=False)
class SandboxList(Model):
    items: list[SandboxBody]
    next_cursor: str | None

@dataclass(kw_only=True, repr=False)
class OperationList(Model):
    items: list[OperationBody]
    next_cursor: str | None

@dataclass(kw_only=True, repr=False)
class FileCaptureRequest(Model):
    path: str

@dataclass(kw_only=True, repr=False)
class FileCaptureResponse(Model):
    capture: str
    size: int
    sha256: str
    expires_unix_ms: int
    chunk_size: int
    simulated: bool
    guest_reported: bool

@dataclass(kw_only=True, repr=False)
class OutputStats(Model):
    seen: int
    stored: int
    truncated: bool

@dataclass(kw_only=True, repr=False)
class OutputEvent(Model):
    stream: str
    offset: int
    next_offset: int
    data_base64: str
    at_end: bool
    complete: bool
    seen: int
    stored: int
    truncated: bool
    simulated: bool
    guest_reported: bool

@dataclass(kw_only=True, repr=False)
class EndEvent(Model):
    reason: str
    stdout: OutputStats
    stderr: OutputStats
    simulated: bool
    guest_reported: bool

@dataclass(kw_only=True, repr=False)
class StreamProblemEvent(Model):
    code: str

MODELS = {'RequestedResources': RequestedResources, 'CreateRequest': CreateRequest, 'CommandInput': CommandInput, 'DestroyRequest': DestroyRequest, 'CancelRequest': CancelRequest, 'AdmittedResponse': AdmittedResponse, 'ProblemBody': ProblemBody, 'OperationBody': OperationBody, 'SandboxBody': SandboxBody, 'SandboxList': SandboxList, 'OperationList': OperationList, 'FileCaptureRequest': FileCaptureRequest, 'FileCaptureResponse': FileCaptureResponse, 'OutputStats': OutputStats, 'OutputEvent': OutputEvent, 'EndEvent': EndEvent, 'StreamProblemEvent': StreamProblemEvent}
