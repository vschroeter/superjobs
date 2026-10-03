"""JobCLI request input preparation tests (issue #39)."""

from __future__ import annotations

import enum
import json
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from pydantic import BaseModel, field_validator
from typer.testing import CliRunner

from superjobs import Job, JobContext
from superjobs.cli import (
    CLIField,
    EXIT_RUNTIME_FAILURE,
    EXIT_USAGE,
    JobCLI,
)
from superjobs.cli.constants import UNAVAILABLE_EXECUTION_MESSAGE
from superjobs.cli.schema_plan import build_command_input_plan
from superjobs.cli.strict_json import CLIInputError, parse_strict_json
from superjobs.payload.codec.implementations.msgpack import MsgpackCodec
from superjobs.payload.codec.payloadcodec import PayloadCodec


@dataclass
class FlatRequest:
    name: str
    count: int = 0
    active: bool = False


@dataclass
class FlatResult:
    ok: bool


class Stage(enum.Enum):
    ALPHA = "alpha"
    BETA = "beta"


class EnumRequest(BaseModel):
    stage: Stage


class NestedRequest(BaseModel):
    nested: dict[str, int]


def _flat_job() -> Job[FlatRequest, FlatResult, None]:
    return Job("tests.cli.flat", version="v1", request=FlatRequest, result=FlatResult)


def _build_flat_cli() -> JobCLI:
    cli = JobCLI()

    async def handler(request: FlatRequest, context: JobContext[None]) -> FlatResult:
        return FlatResult(ok=request.active)

    cli.add("flat", _flat_job(), handler=handler)
    return cli


def test_json_and_field_mode_equivalent() -> None:
    cli = _build_flat_cli()
    runner = CliRunner()
    payload = json.dumps({"name": "a", "count": 2, "active": True})
    for argv in (
        ["submit", "flat", "--json", payload],
        ["submit", "flat", "--name", "a", "--count", "2", "--active"],
    ):
        result = runner.invoke(cli.build_typer(), argv)
        assert result.exit_code == EXIT_RUNTIME_FAILURE
        assert UNAVAILABLE_EXECUTION_MESSAGE in result.stderr


def test_json_input_file_and_stdin(tmp_path: Path) -> None:
    cli = _build_flat_cli()
    runner = CliRunner()
    payload = '{"name": "file", "count": 1}'
    path = tmp_path / "req.json"
    path.write_text(payload, encoding="utf-8")
    file_result = runner.invoke(cli.build_typer(), ["submit", "flat", "--input", str(path)])
    stdin_result = runner.invoke(
        cli.build_typer(),
        ["submit", "flat", "--input", "-"],
        input=payload,
    )
    assert file_result.exit_code == EXIT_RUNTIME_FAILURE
    assert stdin_result.exit_code == EXIT_RUNTIME_FAILURE


def test_mutually_exclusive_sources_rejected() -> None:
    cli = _build_flat_cli()
    runner = CliRunner()
    result = runner.invoke(
        cli.build_typer(),
        ["submit", "flat", "--json", '{"name":"a"}', "--name", "b"],
    )
    assert result.exit_code == EXIT_USAGE
    assert "cannot be combined" in result.stderr


def test_missing_required_field_mode_usage() -> None:
    cli = _build_flat_cli()
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["submit", "flat", "--count", "1"])
    assert result.exit_code == EXIT_USAGE
    assert "missing required" in result.stderr


def test_explicit_false_boolean_preserved_via_cli() -> None:
    cli = _build_flat_cli()
    runner = CliRunner()
    with_active = runner.invoke(
        cli.build_typer(),
        ["submit", "flat", "--name", "x", "--no-active"],
    )
    omitted = runner.invoke(cli.build_typer(), ["submit", "flat", "--name", "x"])
    assert with_active.exit_code == EXIT_RUNTIME_FAILURE
    assert omitted.exit_code == EXIT_RUNTIME_FAILURE


def test_strict_json_rejects_duplicate_keys_and_non_finite() -> None:
    with pytest.raises(CLIInputError, match="duplicate"):
        parse_strict_json('{"a": 1, "a": 2}')
    with pytest.raises(CLIInputError):
        parse_strict_json("1e999")


def test_nested_request_json_only_help() -> None:
    job = Job("tests.cli.nested", version="v1", request=NestedRequest, result=FlatResult)
    cli = JobCLI()

    async def handler(request: NestedRequest, context: JobContext[None]) -> FlatResult:
        return FlatResult(ok=True)

    cli.add("nested", job, handler=handler)
    runner = CliRunner()
    help_result = runner.invoke(cli.build_typer(), ["submit", "nested", "--help"])
    assert help_result.exit_code == 0
    assert "--json" in help_result.stdout
    assert "--nested" not in help_result.stdout
    json_ok = runner.invoke(
        cli.build_typer(),
        ["submit", "nested", "--json", '{"nested": {"a": 1}}'],
    )
    assert json_ok.exit_code == EXIT_RUNTIME_FAILURE


def test_enum_field_option() -> None:
    job = Job("tests.cli.enum", version="v1", request=EnumRequest, result=FlatResult)
    cli = JobCLI()

    async def handler(request: EnumRequest, context: JobContext[None]) -> FlatResult:
        return FlatResult(ok=request.stage is Stage.ALPHA)

    cli.add("enum-cmd", job, handler=handler)
    runner = CliRunner()
    ok = runner.invoke(cli.build_typer(), ["submit", "enum-cmd", "--stage", "alpha"])
    bad = runner.invoke(cli.build_typer(), ["submit", "enum-cmd", "--stage", "gamma"])
    assert ok.exit_code == EXIT_RUNTIME_FAILURE
    assert bad.exit_code == EXIT_USAGE


def test_reserved_collision_resolved_with_field_option() -> None:
    class InputFieldRequest(BaseModel):
        input: str

    class InputFieldResult(BaseModel):
        ok: bool

    job = Job(
        "tests.cli.input-alias",
        version="v1",
        request=InputFieldRequest,
        result=InputFieldResult,
    )
    cli = JobCLI()

    async def handler(request: InputFieldRequest, context: JobContext[None]) -> InputFieldResult:
        return InputFieldResult(ok=bool(request.input))

    cli.add(
        "alias",
        job,
        handler=handler,
        field_options={"input": CLIField(option="payload-text")},
    )
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["submit", "alias", "--payload-text", "hi"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE


def test_msgpack_codec_still_accepts_json_cli_input() -> None:
    codec = PayloadCodec(_flat_job().request_codec.adapter, MsgpackCodec())
    job = Job(
        "tests.cli.msgpack-flat",
        version="v1",
        request=FlatRequest,
        result=FlatResult,
        request_codec=codec,
    )
    cli = JobCLI()

    async def handler(request: FlatRequest, context: JobContext[None]) -> FlatResult:
        return FlatResult(ok=True)

    cli.add("mp", job, handler=handler)
    runner = CliRunner()
    result = runner.invoke(
        cli.build_typer(),
        ["submit", "mp", "--json", '{"name": "n", "count": 1}'],
    )
    assert result.exit_code == EXIT_RUNTIME_FAILURE


_validator_calls = 0


class ValidatedRequest(BaseModel):
    value: int

    @field_validator("value", mode="after")
    @classmethod
    def check(cls, value: int) -> int:
        global _validator_calls
        _validator_calls += 1
        return value


def test_validator_runs_once_through_public_cli() -> None:
    global _validator_calls
    _validator_calls = 0
    job = Job("tests.cli.validated", version="v1", request=ValidatedRequest, result=FlatResult)
    cli = JobCLI()

    async def handler(request: ValidatedRequest, context: JobContext[None]) -> FlatResult:
        return FlatResult(ok=True)

    cli.add("validated", job, handler=handler)
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["run", "validated", "--json", '{"value": 1}'])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert _validator_calls == 1


_default_factory_calls = 0


def _tags_factory() -> list[str]:
    global _default_factory_calls
    _default_factory_calls += 1
    return ["x"]


@dataclass
class DefaultFactoryRequest:
    name: str
    tags: list[str] = field(default_factory=_tags_factory)


def test_default_factory_not_run_at_registration() -> None:
    global _default_factory_calls
    _default_factory_calls = 0
    job = Job(
        "tests.cli.defaults",
        version="v1",
        request=DefaultFactoryRequest,
        result=FlatResult,
    )
    cli = JobCLI()

    async def handler(request: DefaultFactoryRequest, context: JobContext[None]) -> FlatResult:
        return FlatResult(ok=True)

    cli.add("defaults", job, handler=handler)
    runner = CliRunner()
    runner.invoke(cli.build_typer(), ["submit", "defaults", "--help"])
    assert _default_factory_calls == 0


def test_no_request_job_rejects_payload_flags() -> None:
    job = Job("tests.cli.noreq", version="v1", result=FlatResult)
    cli = JobCLI()

    async def handler(context: JobContext[None]) -> FlatResult:
        return FlatResult(ok=True)

    cli.add("noreq", job, handler=handler)
    runner = CliRunner()
    ok = runner.invoke(cli.build_typer(), ["submit", "noreq"])
    bad = runner.invoke(cli.build_typer(), ["submit", "noreq", "--json", "{}"])
    assert ok.exit_code == EXIT_RUNTIME_FAILURE
    assert bad.exit_code == EXIT_USAGE


class IntLevel(enum.IntEnum):
    LOW = 1
    HIGH = 2


@dataclass
class IntEnumRequest:
    level: IntLevel


def test_int_enum_field_accepts_numeric_cli_value() -> None:
    job = Job("tests.cli.intenum", version="v1", request=IntEnumRequest, result=FlatResult)
    cli = JobCLI()

    async def handler(request: IntEnumRequest, context: JobContext[None]) -> FlatResult:
        return FlatResult(ok=request.level is IntLevel.LOW)

    cli.add("intenum", job, handler=handler)
    runner = CliRunner()
    ok = runner.invoke(cli.build_typer(), ["submit", "intenum", "--level", "1"])
    bad = runner.invoke(cli.build_typer(), ["submit", "intenum", "--level", "9"])
    assert ok.exit_code == EXIT_RUNTIME_FAILURE
    assert bad.exit_code == EXIT_USAGE


def test_strict_json_errors_through_public_cli(tmp_path: Path) -> None:
    cli = _build_flat_cli()
    runner = CliRunner()
    cases = [
        ["submit", "flat", "--json", '{"name":"a","name":"b"}'],
        ["submit", "flat", "--json", "1e999"],
        ["submit", "flat", "--json", "NaN"],
        ["submit", "flat", "--json", "{invalid"],
    ]
    for argv in cases:
        result = runner.invoke(cli.build_typer(), argv)
        assert result.exit_code == EXIT_USAGE
        assert result.stdout == ""

    bad_bytes = tmp_path / "bad.json"
    bad_bytes.write_bytes(b"\xff\xfe")
    file_result = runner.invoke(cli.build_typer(), ["submit", "flat", "--input", str(bad_bytes)])
    assert file_result.exit_code == EXIT_USAGE
