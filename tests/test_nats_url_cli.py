"""NATS URL resolution and built-in submit dispatch (issue #46)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
import typer
from typer.testing import CliRunner

from dataclasses import dataclass

from superjobs import Job, JobContext
from superjobs.cli import EXIT_RUNTIME_FAILURE, EXIT_USAGE, JobCLI
from superjobs.cli.nats_url import resolve_nats_url


@dataclass
class EchoRequest:
    value: int


@dataclass
class EchoResult:
    text: str


def _echo_job(name: str = "tests.url.echo") -> Job[EchoRequest, EchoResult, None]:
    return Job(name, version="v1", request=EchoRequest, result=EchoResult)


@pytest.mark.parametrize(
    ("cli_override", "constructor_url", "env", "expected"),
    [
        ("nats://cli", "nats://ctor", "nats://env", "nats://cli"),
        (None, "nats://ctor", "nats://env", "nats://env"),
        (None, "nats://ctor", "", "nats://ctor"),
        (None, None, "", "nats://localhost:4222"),
    ],
)
def test_resolve_nats_url_precedence(
    cli_override: str | None,
    constructor_url: str | None,
    env: str,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPERJOBS_NATS_URL", raising=False)
    if env:
        monkeypatch.setenv("SUPERJOBS_NATS_URL", env)
    assert (
        resolve_nats_url(
            cli_override=cli_override,
            constructor_url=constructor_url,
            use_ambient_env=True,
        )
        == expected
    )


def test_submit_cli_dispatch_observes_resolved_url(
    stub_builtin_remote_runtime: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[str] = []

    @asynccontextmanager
    async def recording_builtin(
        *,
        constructor_url: str | None,
        cli_override: str | None = None,
    ) -> AsyncIterator[object]:
        captured.append(
            resolve_nats_url(
                cli_override=cli_override,
                constructor_url=constructor_url,
                use_ambient_env=True,
            ),
        )
        raise ConnectionError("stop after url capture")
        yield  # pragma: no cover

    monkeypatch.setattr(
        "superjobs.cli.app.builtin_remote_runtime",
        recording_builtin,
    )
    monkeypatch.setenv("SUPERJOBS_NATS_URL", "nats://from-env")
    cli = JobCLI(nats_url="nats://from-ctor")
    cli.add("echo", Job("tests.url.cli"), remote_only=True)
    result = CliRunner().invoke(
        cli.build_typer(),
        ["submit", "--nats-url", "nats://from-cli", "echo"],
    )
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert captured == ["nats://from-cli"]


def test_submit_invalid_nats_url_option_rejects_before_runtime() -> None:
    cli = JobCLI()
    cli.add("echo", _echo_job("tests.url.invalid"), remote_only=True)

    @asynccontextmanager
    async def probe(
        *,
        constructor_url: str | None,
        cli_override: str | None = None,
    ) -> AsyncIterator[object]:
        assert cli_override == ""
        raise ValueError("nats-url must be non-empty when provided")
        yield  # pragma: no cover

    with patch("superjobs.cli.app.builtin_remote_runtime", probe):
        result = CliRunner().invoke(
            cli.build_typer(),
            ["submit", "--nats-url", "", "echo", "--value", "1"],
        )
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert "nats-url must be non-empty" in result.stderr


def test_submit_help_does_not_start_builtin_remote(
    stub_builtin_remote_runtime: None,
) -> None:
    entered: list[str] = []

    @asynccontextmanager
    async def must_not_run(**_kwargs: object) -> AsyncIterator[object]:
        entered.append("runtime")
        yield object()

    with patch("superjobs.cli.app.builtin_remote_runtime", must_not_run):
        cli = JobCLI()
        cli.add("echo", Job("tests.url.help"), remote_only=True)
        result = CliRunner().invoke(cli.build_typer(), ["submit", "--help"])
    assert result.exit_code == 0
    assert entered == []


def test_run_local_unaffected_by_submit_nats_url_option() -> None:
    from dataclasses import dataclass

    from superjobs import JobContext

    @dataclass
    class EchoRequest:
        value: int

    @dataclass
    class EchoResult:
        text: str

    job = Job("tests.url.local", request=EchoRequest, result=EchoResult)
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text=str(request.value))

    cli.add("echo", job, handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["run", "echo", "--value", "2"])
    assert result.exit_code == 0


def test_custom_remote_factory_rejects_nats_url_before_acquisition() -> None:
    from collections.abc import Callable
    from contextlib import asynccontextmanager

    from superjobs import SuperJobs

    entered: list[str] = []

    @asynccontextmanager
    async def custom() -> AsyncIterator[SuperJobs]:
        entered.append("runtime")
        yield SuperJobs()

    cli = JobCLI(remote_runtime_factory=custom)
    cli.add("echo", Job("tests.url.custom"), remote_only=True)
    result = CliRunner().invoke(
        cli.build_typer(),
        ["submit", "--nats-url", "nats://blocked", "echo"],
    )
    assert result.exit_code == EXIT_USAGE
    assert entered == []


def test_built_submit_state_snapshot_is_independent_per_typer_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[str | None] = []

    @asynccontextmanager
    async def recording_builtin(
        *,
        constructor_url: str | None,
        cli_override: str | None = None,
    ) -> AsyncIterator[object]:
        captured.append(cli_override)
        raise ConnectionError("probe")
        yield  # pragma: no cover

    monkeypatch.setattr(
        "superjobs.cli.app.builtin_remote_runtime",
        recording_builtin,
    )
    cli = JobCLI()
    cli.add("echo", _echo_job("tests.url.snapshot"), remote_only=True)
    first_app = cli.build_typer()
    CliRunner().invoke(
        first_app,
        ["submit", "--nats-url", "nats://first", "echo", "--value", "1"],
    )
    assert captured == ["nats://first"]

    captured.clear()
    second_app = cli.build_typer()
    CliRunner().invoke(second_app, ["submit", "echo", "--value", "1"])
    assert captured == [None]


def test_mount_submit_callback_does_not_replace_application_ctx_obj() -> None:
    owner_state = {"app": True}
    app = typer.Typer()

    @app.callback()
    def _app_callback(ctx: typer.Context) -> None:
        ctx.obj = owner_state

    cli = JobCLI()
    cli.add("echo", Job("tests.url.mount"), remote_only=True)
    cli.mount(app)
    result = CliRunner().invoke(app, ["submit", "--help"])
    assert result.exit_code == 0
    assert owner_state == {"app": True}
