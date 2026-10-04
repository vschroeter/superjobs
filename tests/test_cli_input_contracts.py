"""Observe prepared requests through public CLI and payload-adapter contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel, Field, RootModel, computed_field, model_validator
from typer.testing import CliRunner

from superjobs import Job
from superjobs.cli import CLIField, CLIRegistrationError, JobCLI
from superjobs.payload.codec.implementations.msgpack import MsgpackCodec
from superjobs.payload.codec.payloadcodec import PayloadCodec


_DEFAULT_SCHEMA = object()


class RecordingAdapter:
    def __init__(self, delegate: Any, schema: Any = _DEFAULT_SCHEMA) -> None:
        self.delegate = delegate
        self.schema_value = schema
        self.wire_values: list[Any] = []
        self.loaded: list[Any] = []

    def schema(self) -> Any:
        return self.delegate.schema() if self.schema_value is _DEFAULT_SCHEMA else self.schema_value

    def load(self, value: Any) -> Any:
        self.wire_values.append(value)
        result = self.delegate.load(value)
        self.loaded.append(result)
        return result

    def dump(self, value: Any) -> Any:
        raise AssertionError("CLI input must not round-trip through dump")


def make_cli(request: Any, *, adapter: Any = None, schema: Any = _DEFAULT_SCHEMA,
             **configuration: Any) -> tuple[JobCLI, RecordingAdapter]:
    if adapter is None:
        original = Job("tests.cli.input-contracts", version="v1", request=request)
        adapter = original.request_codec.adapter
    recorder = RecordingAdapter(adapter, schema)
    job = Job("tests.cli.input-contracts", version="v1", request=request,
              request_codec=PayloadCodec(recorder, MsgpackCodec()))

    def forbidden_factory() -> Any:
        raise AssertionError("Input preparation must not initialize a handler or runtime")

    cli = JobCLI(local_runtime_factory=forbidden_factory, remote_runtime_factory=forbidden_factory)
    cli.add("probe", job, handler_factory=forbidden_factory, **configuration)
    return cli, recorder


def invoke(cli: JobCLI, mode: str, args: list[str], code: int = 1, **kwargs: Any) -> Any:
    result = CliRunner().invoke(cli.build_typer(), [mode, "probe", *args], **kwargs)
    assert result.exit_code == code, (result.output, result.exception)
    assert result.stdout == ""
    assert result.stderr
    assert "Traceback" not in result.stderr
    return result


class Color(str, Enum):
    RED = "red"
    BLUE = "blue"


class Level(IntEnum):
    LOW = 1
    HIGH = 2


class Fraction(float, Enum):
    HALF = 0.5
    WHOLE = 1.0


@dataclass
class Flat:
    name: str
    count: int = 0
    active: bool = True
    ratio: float = 0.5
    color: Color = Color.RED
    level: Level = Level.LOW
    fraction: Fraction = Fraction.HALF


@pytest.mark.parametrize("mode", ["run", "submit"])
def test_equivalent_typed_requests_in_all_input_forms(mode: str, tmp_path: Any) -> None:
    cli, recorder = make_cli(Flat)
    tree = {"name": "Ada 🐍", "count": 2, "active": False, "ratio": 1.25,
            "color": "blue", "level": 2, "fraction": 1.0}
    text = json.dumps(tree, ensure_ascii=False)
    path = tmp_path / "request.json"
    path.write_text(text, encoding="utf-8")
    cases = [(["--json", text], {}), (["--input", str(path)], {}),
             (["--input", "-"], {"input": text}),
             (["--name", tree["name"], "--count", "2", "--no-active", "--ratio", "1.25",
               "--color", "blue", "--level", "2", "--fraction", "1.0"], {})]
    for args, kwargs in cases:
        invoke(cli, mode, args, **kwargs)
    assert recorder.wire_values == [tree] * 4
    expected = Flat(tree["name"], 2, False, 1.25, Color.BLUE, Level.HIGH, Fraction.WHOLE)
    assert recorder.loaded == [expected] * 4


@pytest.mark.parametrize("mode", ["run", "submit"])
def test_defaults_false_and_explicit_default_values(mode: str) -> None:
    cli, recorder = make_cli(Flat)
    invoke(cli, mode, ["--name", "Ada"])
    invoke(cli, mode, ["--name", "Ada", "--no-active", "--count", "0"])
    assert recorder.wire_values == [{"name": "Ada"}, {"name": "Ada", "active": False, "count": 0}]
    assert recorder.loaded == [Flat("Ada"), Flat("Ada", active=False)]


def test_help_preserves_default_and_description_beside_enum_reference() -> None:
    class Described(BaseModel):
        color: Color = Field(default=Color.BLUE, description="Select a color.")

    cli, recorder = make_cli(Described)
    result = CliRunner().invoke(cli.build_typer(), ["submit", "probe", "--help"])
    assert result.exit_code == 0
    help_text = " ".join(result.stdout.split())
    assert "Select a color." in help_text
    assert "default:" in help_text
    assert "'blue'" in help_text
    assert "choices:" in help_text
    assert "red" in help_text and "blue" in help_text
    assert recorder.loaded == []


def test_dataclass_default_factory_runs_once_for_json_and_fields() -> None:
    calls: list[int] = []

    def default() -> int:
        calls.append(1)
        return 5

    @dataclass
    class WithDefault:
        name: str
        count: int = field(default_factory=default)

    cli, recorder = make_cli(WithDefault)
    CliRunner().invoke(cli.build_typer(), ["submit", "probe", "--help"])
    assert calls == []
    for args in (["--name", "Ada"], ["--json", '{"name":"Ada"}']):
        calls.clear()
        invoke(cli, "submit", args)
        assert calls == [1]
        assert recorder.loaded[-1].count == 5


@pytest.mark.parametrize("mode", ["run", "submit"])
@pytest.mark.parametrize("field_args", [["--name", "Ada"], ["--no-active"],
                                        ["--active"], ["--count", "0"]])
@pytest.mark.parametrize("source", ["json", "input"])
def test_source_conflicts_even_for_false_or_default_equal_values(
    mode: str, field_args: list[str], source: str,
) -> None:
    cli, recorder = make_cli(Flat)
    args = ["--json", '{"name":"Ada"}'] if source == "json" else ["--input", "-"]
    result = invoke(cli, mode, [*args, *field_args], 2, input='{"name":"Ada"}')
    assert "cannot be combined" in result.stderr
    assert recorder.wire_values == []


@pytest.mark.parametrize("mode", ["run", "submit"])
def test_whole_request_sources_are_exclusive(mode: str) -> None:
    cli, recorder = make_cli(Flat)
    invoke(cli, mode, ["--json", '{"name":"Ada"}', "--input", "-"], 2)
    assert recorder.wire_values == []


class Canonical(BaseModel):
    count: int = Field(validation_alias="incoming", serialization_alias="outgoing", ge=1,
                       description="Positive count.")
    label: str = Field(min_length=2)


@pytest.mark.parametrize("mode", ["run", "submit"])
def test_canonical_names_constraints_and_strict_json(mode: str) -> None:
    cli, recorder = make_cli(Canonical)
    for args in [["--count", "3", "--label", "ok"],
                 ["--json", '{"count":3,"label":"ok"}']]:
        invoke(cli, mode, args)
    assert [value.count for value in recorder.loaded] == [3, 3]
    for args in [["--json", '{"incoming":3,"label":"ok"}'],
                 ["--json", '{"outgoing":3,"label":"ok"}'],
                 ["--json", '{"count":"3","label":"ok"}'],
                 ["--json", '{"count":3,"label":"ok","extra":1}'],
                 ["--count", "0", "--label", "ok"],
                 ["--count", "3", "--label", "x"], ["--count", "3"]]:
        invoke(cli, mode, args, 2)
    assert len(recorder.loaded) == 2


@pytest.mark.parametrize("args", [["--ratio", "nan"], ["--ratio", "inf"],
                                  ["--level", "3"], ["--color", "other"],
                                  ["--fraction", "2.0"], ["--count", "1.2"],
                                  ["--unknown", "1"]])
def test_bad_field_values_have_usage_diagnostics(args: list[str]) -> None:
    cli, recorder = make_cli(Flat)
    invoke(cli, "submit", ["--name", "Ada", *args], 2)
    assert recorder.loaded == []


@pytest.mark.parametrize("mode", ["run", "submit"])
@pytest.mark.parametrize("text", ["{", "NaN", "Infinity", "-Infinity", "1e999",
                                  '{"name":"a","name":"b"}',
                                  '{"nested":{"x":1,"x":2}}'])
def test_json_parser_errors_precede_adapter_loading(mode: str, text: str) -> None:
    cli, recorder = make_cli(Flat)
    invoke(cli, mode, ["--json", text], 2)
    assert recorder.wire_values == []


@pytest.mark.parametrize("mode", ["run", "submit"])
def test_file_stdin_and_utf8_failures(mode: str, tmp_path: Any) -> None:
    cli, recorder = make_cli(Flat)
    path = tmp_path / "invalid.json"
    path.write_bytes(b"\xff")
    invoke(cli, mode, ["--input", str(path)], 2)
    invoke(cli, mode, ["--input", str(tmp_path / "missing")], 2)
    invoke(cli, mode, ["--input", str(tmp_path)], 2)
    invoke(cli, mode, ["--input", "\x00"], 2)
    invoke(cli, mode, ["--input", "-"], 2, input=b"\xff")
    assert recorder.wire_values == []


@dataclass
class Pair:
    first: str
    second: int
    active: bool = False


@pytest.mark.parametrize("mode", ["run", "submit"])
def test_positionals_have_explicit_order_and_json_alternative(mode: str) -> None:
    with pytest.raises(CLIRegistrationError):
        make_cli(Pair, positional_fields=("first",),
                 field_options={"first": CLIField(option="--bad")})
    cli, recorder = make_cli(Pair, positional_fields=("second", "first"),
                             field_options={"first": CLIField(help="First text.")})
    invoke(cli, mode, ["7", "hello", "--active"])
    invoke(cli, mode, ["--json", '{"first":"hello","second":7,"active":true}'])
    assert recorder.loaded == [Pair("hello", 7, True)] * 2
    invoke(cli, mode, [], 2)
    invoke(cli, mode, ["7"], 2)
    invoke(cli, mode, ["7", "hello", "--json", '{}'], 2)
    invoke(cli, mode, ["7", "hello", "--input", "-"], 2)
    help_result = CliRunner().invoke(cli.build_typer(), [mode, "probe", "--help"])
    assert help_result.exit_code == 0
    assert "First text." in help_result.stdout
    assert "required in field mode" in help_result.stdout


class CustomAdapter:
    def __init__(self, schema: Any = None) -> None:
        self.schema_value = schema

    def schema(self) -> Any:
        return self.schema_value

    def load(self, value: Any) -> Any:
        if value == "bad":
            raise ValueError("invalid custom request")
        return value


@pytest.mark.parametrize("mode", ["run", "submit"])
def test_schema_less_adapter_retains_null_and_scalar_inputs(mode: str) -> None:
    cli, recorder = make_cli(str, adapter=CustomAdapter())
    invoke(cli, mode, ["--json", "null"])
    invoke(cli, mode, ["--json", '"ok"'])
    invoke(cli, mode, ["--json", '"bad"'], 2)
    invoke(cli, mode, [], 2)
    assert recorder.loaded == [None, "ok"]


@pytest.mark.parametrize("schema,tree", [
    ({"type":"object", "properties":{"name":{"type":"string"},
      "nested":{"type":"object"}}}, {"name":"a", "nested":{}}),
    ({"type":"object", "properties":{"items":{"type":"array"}}}, {"items":[]}),
    ({"type":"object", "properties":{"value":{"anyOf":[{"type":"string"},{"type":"null"}]}}}, {"value":None}),
    ({"type":"object", "properties":{"value":{"anyOf":[{"type":"string"},{"type":"integer"}]}}}, {"value":1}),
    ({"type":"object", "additionalProperties":{"type":"string"}}, {"arbitrary":"ok"}),
    ({"anyOf":[{"$ref":"#/$defs/Model"},{"type":"null"}],
      "$defs":{"Model":{"type":"object", "properties":{"name":{"type":"string"}}}}}, None),
])
def test_unsupported_shapes_are_command_wide_json_only(schema: Any, tree: Any) -> None:
    cli, recorder = make_cli(dict, adapter=CustomAdapter(schema))
    for mode in ("run", "submit"):
        invoke(cli, mode, ["--json", json.dumps(tree)])
        invoke(cli, mode, [], 2)
        help_result = CliRunner().invoke(cli.build_typer(), [mode, "probe", "--help"])
        assert "--json" in help_result.stdout
        assert "--name" not in help_result.stdout
        assert "--value" not in help_result.stdout
    assert recorder.loaded == [tree, tree]


class NullableRequest(RootModel[int | None]):
    pass


@pytest.mark.parametrize("request_type,text,expected", [(int,"3",3),
                          (NullableRequest,"null",NullableRequest(None))])
def test_builtin_scalar_and_nullable_request(request_type: Any, text: str, expected: Any) -> None:
    cli, recorder = make_cli(request_type)
    invoke(cli, "submit", ["--json", text])
    invoke(cli, "submit", [], 2)
    assert recorder.loaded == [expected]


class Computed(BaseModel):
    count: int

    @computed_field
    @property
    def double(self) -> int:
        return self.count * 2


def test_serialization_only_fields_do_not_become_required_input() -> None:
    cli, recorder = make_cli(Computed)
    invoke(cli, "submit", ["--count", "2"])
    assert recorder.loaded[0].count == 2
    help_result = CliRunner().invoke(cli.build_typer(), ["submit", "probe", "--help"])
    assert "--double" not in help_result.stdout


def test_hooks_and_default_factories_run_once_only_during_input() -> None:
    calls: list[str] = []

    def default() -> int:
        calls.append("default")
        return 5

    class Validated(BaseModel):
        name: str
        count: int = Field(default_factory=default)

        @model_validator(mode="after")
        def record(self) -> Any:
            calls.append("validator")
            return self

    cli, recorder = make_cli(Validated)
    CliRunner().invoke(cli.build_typer(), ["submit", "probe", "--help"])
    assert calls == []
    for mode in ("run", "submit"):
        for args in (["--name", "Ada"], ["--json", '{"name":"Ada"}']):
            calls.clear()
            invoke(cli, mode, args)
            assert calls == ["default", "validator"]
            assert recorder.loaded[-1].count == 5


def test_effective_collisions_and_positionals_with_reserved_names() -> None:
    schema = {"type":"object", "properties":{"a_b":{"type":"string"}, "a-b":{"type":"string"}}}
    with pytest.raises(CLIRegistrationError, match="duplicate normalized"):
        make_cli(dict, adapter=CustomAdapter(schema))
    cli, recorder = make_cli(dict, adapter=CustomAdapter(schema),
                             field_options={"a-b":CLIField(option="other", help="Other text.")})
    invoke(cli, "submit", ["--a-b", "first", "--other", "second"])
    assert recorder.loaded == [{"a_b":"first", "a-b":"second"}]

    schema = {"type":"object", "properties":{"input":{"type":"boolean"}, "no_input":{"type":"string"}}}
    cli, recorder = make_cli(dict, adapter=CustomAdapter(schema), positional_fields=("input",))
    invoke(cli, "submit", ["false", "--no-input", "text"])
    assert recorder.loaded == [{"input":False, "no_input":"text"}]
    with pytest.raises(CLIRegistrationError):
        make_cli(dict, adapter=CustomAdapter(schema))

    schema = {"type":"object", "properties":{"x":{"type":"string"}, "no_x":{"type":"string"}}}
    make_cli(dict, adapter=CustomAdapter(schema))
    schema["properties"]["x"] = {"type":"boolean"}
    with pytest.raises(CLIRegistrationError, match="negative name"):
        make_cli(dict, adapter=CustomAdapter(schema))


@pytest.mark.parametrize("configuration", [
    {"positional_fields":["name"]}, {"positional_fields":(42,)},
    {"positional_fields":("name","name")}, {"positional_fields":("missing",)},
    {"field_options":{"missing":CLIField()}}, {"field_options":{"name":"wrong"}},
    {"field_options":"wrong"}, {"field_options":{"name":CLIField(option="--bad")}},
    {"field_options":{"name":CLIField(option="help")}},
    {"field_options":{"name":CLIField(option=42)}}, {"field_options":{"name":CLIField(help=42)}},
])
def test_invalid_configuration_is_registration_error(configuration: dict[str, Any]) -> None:
    with pytest.raises(CLIRegistrationError):
        make_cli(Flat, **configuration)


def test_schema_less_and_unsupported_configuration_is_not_ignored() -> None:
    with pytest.raises(CLIRegistrationError):
        make_cli(str, adapter=CustomAdapter(), field_options={"x":CLIField()})
    with pytest.raises(CLIRegistrationError):
        make_cli(int, positional_fields=("x",))


@dataclass
class Empty:
    pass


def test_empty_flat_object_can_use_implicit_empty_request() -> None:
    cli, recorder = make_cli(Empty)
    invoke(cli, "submit", [])
    assert recorder.loaded == [Empty()]


def test_main_reports_stdin_read_failure_as_usage(capsys: Any) -> None:
    class BrokenStdin:
        @property
        def buffer(self) -> Any:
            return self

        def read(self) -> bytes:
            raise OSError("read failed")

    cli, recorder = make_cli(Flat)
    with patch("sys.stdin", BrokenStdin()):
        assert cli.main(["submit", "probe", "--input", "-"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "cannot read stdin" in captured.err
    assert recorder.wire_values == []
