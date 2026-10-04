"""Public JobCLI registration and Typer shell tests (issue #38)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from unittest.mock import patch

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from superjobs import Job, JobContext
from superjobs.cli import (
    CLIRegistrationError,
    EXIT_INTERRUPTED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_USAGE,
    JobCLI,
)
from superjobs.cli.constants import MISSING_REMOTE_FACTORY_MESSAGE, REMOTE_ONLY_RUN_MESSAGE


@dataclass
class EchoRequest:
    value: int


@dataclass
class EchoResult:
    text: str


class InputFieldRequest(BaseModel):
    input: str


class InputFieldResult(BaseModel):
    ok: bool


def _echo_job() -> Job[EchoRequest, EchoResult, None]:
    return Job("tests.cli.echo", version="v1", request=EchoRequest, result=EchoResult)


def _collision_job() -> Job[InputFieldRequest, InputFieldResult, None]:
    return Job(
        "tests.cli.collision",
        version="v1",
        request=InputFieldRequest,
        result=InputFieldResult,
    )


def _remote_cli() -> JobCLI:
    cli = JobCLI()
    cli.add("remote", Job("tests.cli.remote", version="v1"), remote_only=True)
    return cli


def test_missing_cli_extra_reports_install_instruction() -> None:
    code = """
import importlib.abc
import sys
class BlockTyper(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'typer':
            raise ModuleNotFoundError('No module named typer', name='typer')
sys.meta_path.insert(0, BlockTyper())
import superjobs
try:
    import superjobs.cli
except ImportError as error:
    assert "optional 'cli' extra" in str(error), error
    assert "superjobs[cli]" in str(error), error
else:
    raise AssertionError('CLI extra unexpectedly available')
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_core_import_does_not_load_cli_machinery() -> None:
    script = (
        "import superjobs, sys; "
        "assert 'superjobs.cli' not in sys.modules; "
        "assert 'typer' not in sys.modules"
    )
    env = os.environ.copy()
    env["PYTHONNOUSERSITE"] = "1"
    subprocess.run([sys.executable, "-c", script], check=True, env=env)


@pytest.mark.parametrize(
    ("argv", "expected_code"),
    [
        (["submit", "remote"], EXIT_RUNTIME_FAILURE),
        (["run", "remote"], EXIT_USAGE),
        (["nonesuch"], EXIT_USAGE),
        (["run", "remote", "--nope"], EXIT_USAGE),
        (["--help"], EXIT_SUCCESS),
    ],
)
def test_main_exit_codes(argv: list[str], expected_code: int) -> None:
    cli = _remote_cli()
    assert cli.main(argv) == expected_code


def test_main_no_args_usage_exit() -> None:
    cli = _remote_cli()
    assert cli.main([]) == EXIT_USAGE


def test_main_unknown_command_no_traceback(capsys: pytest.CaptureFixture[str]) -> None:
    cli = _remote_cli()
    code = cli.main(["nonesuch"])
    assert code == EXIT_USAGE
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    assert "No such command" in captured.err
    assert captured.out == ""


def test_main_keyboard_interrupt_exit() -> None:
    cli = _remote_cli()
    with patch("typer.Typer.__call__", side_effect=KeyboardInterrupt):
        assert cli.main(["submit", "remote"]) == EXIT_INTERRUPTED


def test_main_process_submit_remote() -> None:
    script = textwrap.dedent(
        """
        from superjobs import Job
        from superjobs.cli import JobCLI, EXIT_RUNTIME_FAILURE
        cli = JobCLI()
        cli.add("remote", Job("tests.cli.remote.proc"), remote_only=True)
        raise SystemExit(cli.main(["submit", "remote"]))
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONNOUSERSITE": "1"},
    )
    assert result.returncode == EXIT_RUNTIME_FAILURE
    assert MISSING_REMOTE_FACTORY_MESSAGE in result.stderr


def test_main_process_run_remote_only() -> None:
    script = textwrap.dedent(
        """
        from superjobs import Job
        from superjobs.cli import JobCLI, EXIT_USAGE
        cli = JobCLI()
        cli.add("remote", Job("tests.cli.remote.proc"), remote_only=True)
        raise SystemExit(cli.main(["run", "remote"]))
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONNOUSERSITE": "1"},
    )
    assert result.returncode == EXIT_USAGE
    assert REMOTE_ONLY_RUN_MESSAGE in result.stderr


def test_help_lists_registered_commands() -> None:
    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text=str(request.value))

    cli.add("echo", job, handler=handler)
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["run", "--help"])
    assert result.exit_code == 0
    assert "echo" in result.stdout


def test_build_typer_snapshots_registrations() -> None:
    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    cli.add("echo", job, handler=handler)
    snapshot = cli.build_typer()
    cli.add("later", job, handler=handler)
    runner = CliRunner()
    before = runner.invoke(snapshot, ["run", "--help"])
    after = runner.invoke(cli.build_typer(), ["run", "--help"])
    assert "later" not in before.stdout
    assert "later" in after.stdout


def test_duplicate_command_name_rejected() -> None:
    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    cli.add("echo", job, handler=handler)
    with pytest.raises(CLIRegistrationError, match="duplicate"):
        cli.add("echo", job, handler=handler)


def test_reserved_option_collision_rejected() -> None:
    job = _collision_job()
    cli = JobCLI()

    async def handler(
        request: InputFieldRequest,
        context: JobContext[None],
    ) -> InputFieldResult:
        return InputFieldResult(ok=True)

    with pytest.raises(CLIRegistrationError, match="reserved CLI options"):
        cli.add("collide", job, handler=handler)


def test_duplicate_normalized_option_names_rejected() -> None:
    job = Job("tests.cli.dup-norm", version="v1", request=EchoRequest, result=EchoResult)
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    schema = {
        "type": "object",
        "properties": {
            "foo_bar": {"type": "integer"},
            "foo-bar": {"type": "integer"},
        },
    }
    object.__setattr__(job.request_codec, "schema", lambda: schema)  # type: ignore[method-assign]

    with pytest.raises(CLIRegistrationError, match="duplicate normalized"):
        cli.add("dup", job, handler=handler)


def test_invalid_command_name_rejected() -> None:
    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    with pytest.raises(CLIRegistrationError, match="command_name"):
        cli.add("Bad_Name", job, handler=handler)


def test_handler_job_mismatch_rejected() -> None:
    job_a = _echo_job()
    job_b = Job(
        "tests.cli.other",
        version="v1",
        request=EchoRequest,
        result=EchoResult,
    )
    cli = JobCLI()

    @job_b.handler
    async def marked(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    with pytest.raises(CLIRegistrationError, match="marked"):
        cli.add("echo", job_a, handler=marked)


def test_lazy_handler_factory_not_called_for_help() -> None:
    job = _echo_job()
    cli = JobCLI()
    calls = 0

    def factory() -> Callable[[EchoRequest, JobContext[None]], Awaitable[EchoResult]]:
        nonlocal calls
        calls += 1

        async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
            return EchoResult(text="lazy")

        return handler

    cli.add("echo", job, handler_factory=factory)
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["submit", "--help"])
    assert result.exit_code == 0
    assert calls == 0


def test_remote_only_help_does_not_call_local_factory() -> None:
    job = _echo_job()
    calls = 0

    def local_factory():
        nonlocal calls
        calls += 1
        raise RuntimeError("should not run")

    cli = JobCLI(local_runtime_factory=local_factory)
    cli.add("echo", job, remote_only=True)
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["run", "echo"])
    assert result.exit_code == EXIT_USAGE
    assert REMOTE_ONLY_RUN_MESSAGE in result.stderr
    assert calls == 0


def test_run_local_uses_default_in_memory_factory() -> None:
    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text=str(request.value))

    cli.add("echo", job, handler=handler)
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["run", "echo", "--value", "1"])
    assert result.exit_code == EXIT_SUCCESS
    assert json.loads(result.stdout) == {"text": "1"}


def test_submit_reports_unavailable() -> None:
    job = _echo_job()
    cli = JobCLI()
    cli.add("echo", job, remote_only=True)
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["submit", "echo", "--value", "1"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE
    assert MISSING_REMOTE_FACTORY_MESSAGE in result.stderr


def test_main_rejects_active_event_loop() -> None:
    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    cli.add("echo", job, handler=handler)

    async def inside_loop() -> None:
        code = cli.main([])
        assert code == EXIT_USAGE

    import asyncio

    asyncio.run(inside_loop())


def test_build_typer_snapshots_local_runtime_factory() -> None:
    from contextlib import asynccontextmanager

    from superjobs import InMemoryTransport, SuperJobs

    job = _echo_job()
    calls: list[str] = []

    @asynccontextmanager
    async def first_factory():
        calls.append("first")
        jobs = SuperJobs(transport=InMemoryTransport())
        async with jobs:
            yield jobs

    @asynccontextmanager
    async def second_factory():
        calls.append("second")
        jobs = SuperJobs(transport=InMemoryTransport())
        async with jobs:
            yield jobs

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="ok")

    cli = JobCLI(local_runtime_factory=first_factory)
    cli.add("echo", job, handler=handler)
    app = cli.build_typer()
    cli.local_runtime_factory = second_factory
    result = CliRunner().invoke(app, ["run", "echo", "--value", "1"])
    assert result.exit_code == EXIT_SUCCESS
    assert calls == ["first"]


def test_submit_help_notes_remote_runtime_factory() -> None:
    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    cli.add("echo", job, handler=handler)
    result = CliRunner().invoke(cli.build_typer(), ["submit", "echo", "--help"])
    assert result.exit_code == EXIT_SUCCESS
    assert "remote_runtime_factory" in " ".join(result.stdout.split())


def test_mount_on_application_typer() -> None:
    import typer

    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    cli.add("echo", job, handler=handler)
    app = typer.Typer()
    cli.mount(app)
    runner = CliRunner()
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    assert "echo" in result.stdout


def test_mount_rejects_existing_run_group() -> None:
    import typer

    job = _echo_job()
    cli = JobCLI()

    async def handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="x")

    cli.add("echo", job, handler=handler)
    app = typer.Typer()
    run_group = typer.Typer()

    @run_group.command("noop")
    def noop() -> None:
        pass

    app.add_typer(run_group, name="run")
    with pytest.raises(CLIRegistrationError, match="'run'"):
        cli.mount(app)


def test_marked_handler_registration() -> None:
    job = _echo_job()
    cli = JobCLI()

    @job.handler
    async def marked(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="marked")

    cli.add("echo", job, handler=marked)
    assert len(cli.registrations()) == 1


def test_sync_handler_registration() -> None:
    from contextlib import asynccontextmanager

    from superjobs import InMemoryTransport, SuperJobs

    job = _echo_job()

    @asynccontextmanager
    async def local_runtime():
        jobs = SuperJobs(transport=InMemoryTransport())
        async with jobs:
            yield jobs

    cli = JobCLI(local_runtime_factory=local_runtime)

    def sync_handler(request: EchoRequest, context: JobContext[None]) -> EchoResult:
        return EchoResult(text="sync")

    cli.add("echo", job, handler=sync_handler)
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["run", "echo", "--value", "1"])
    assert result.exit_code == EXIT_SUCCESS


def test_no_request_job_registration() -> None:
    job = Job("tests.cli.noop", version="v1", result=EchoResult)
    cli = JobCLI()

    async def handler(context: JobContext[None]) -> EchoResult:
        return EchoResult(text="noop")

    cli.add("noop", job, handler=handler)
    runner = CliRunner()
    result = runner.invoke(cli.build_typer(), ["submit", "noop"])
    assert result.exit_code == EXIT_RUNTIME_FAILURE

@pytest.mark.parametrize("name", ["run", "submit"])
@pytest.mark.parametrize("kind", ["explicit-command", "inferred-command", "named-group", "flattened-group"])
def test_mount_rejects_resolved_host_names_without_mutation(name: str, kind: str) -> None:
    import typer

    app = typer.Typer()
    def host_command() -> None:
        raise AssertionError("Mount must not invoke a host command")
    host_command.__name__ = name
    if kind == "explicit-command":
        app.command(name)(host_command)
    elif kind == "inferred-command":
        app.command()(host_command)
    else:
        group = typer.Typer(name=name if kind == "named-group" else None)
        group.command(name if kind == "flattened-group" else "example")(host_command)
        app.add_typer(group)
    before = (tuple(app.registered_commands), tuple(app.registered_groups))
    with pytest.raises(CLIRegistrationError, match=name):
        _remote_cli().mount(app)
    assert before == (tuple(app.registered_commands), tuple(app.registered_groups))


def test_submit_initializes_remote_runtime_not_local_handler(
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[str] = []

    def runtime_factory():
        calls.append("runtime")
        raise AssertionError("remote runtime startup failed in test")

    def handler_factory():
        calls.append("handler")
        raise AssertionError("submit must not load worker code")

    cli = JobCLI(local_runtime_factory=runtime_factory, remote_runtime_factory=runtime_factory)
    cli.add("lazy", Job("tests.cli.lazy.unavailable"), handler_factory=handler_factory)
    assert cli.main(["submit", "lazy"]) == EXIT_RUNTIME_FAILURE
    assert calls == ["runtime"]
    assert capsys.readouterr().out == ""


def test_run_initializes_local_runtime_for_selected_command() -> None:
    calls: list[str] = []

    def runtime_factory():
        calls.append("runtime")
        raise AssertionError("handler factory must not run when runtime fails")

    def handler_factory():
        calls.append("handler")
        raise AssertionError("handler factory must not run when runtime fails")

    cli = JobCLI(local_runtime_factory=runtime_factory)
    cli.add("lazy", Job("tests.cli.lazy.unavailable"), handler_factory=handler_factory)
    assert cli.main(["run", "lazy"]) == EXIT_RUNTIME_FAILURE
    assert calls == ["runtime"]


@pytest.mark.parametrize("argv, expected", [
    (["run", "greet", "world"], EXIT_SUCCESS),
    (["submit", "greet-remote", "--json", '{"name":"world"}'], EXIT_RUNTIME_FAILURE),
    (["run", "greet-remote", "--json", '{"name":"world"}'], EXIT_USAGE),
    (["missing"], EXIT_USAGE),
    (["submit", "greet-remote", "--bad"], EXIT_USAGE),
    (["--help"], EXIT_SUCCESS),
])
def test_example_process_exit_conventions(argv: list[str], expected: int) -> None:
    # Installed-wheel tests copy the example with the test module; source tests
    # locate the same application in the repository.
    from pathlib import Path
    example = Path(__file__).with_name("cli_registration_example.py")
    if not example.exists():
        example = Path(__file__).resolve().parents[1] / "examples/cli_registration/main.py"
    result = subprocess.run(
        [sys.executable, str(example), *argv],
        # Remote startup and cleanup each have a 30-second budget; include
        # interpreter startup overhead when checking the process exit.
        capture_output=True, text=True, timeout=65,
        env={
            **os.environ,
            "PYTHONNOUSERSITE": "1",
            "SUPERJOBS_NATS_URL": "nats://127.0.0.1:1",
        },
    )
    assert result.returncode == expected
    assert "Traceback" not in result.stderr
    if expected == EXIT_SUCCESS and argv[:2] == ["run", "greet"]:
        assert json.loads(result.stdout)["message"]
    elif expected:
        assert result.stdout == ""
        assert result.stderr.strip()
