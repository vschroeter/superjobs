# Statische Typgarantien: Bestandsaufnahme

Stand: 2026-09-28, Branch `api_design`, Library-Commit `d9bdcad`.
Grundlage für [Statische Typgarantien des aktuellen Interfaces vermessen](https://github.com/vschroeter/superjobs/issues/3).

Die Messung wurde von Codex lokal durchgeführt, ohne Änderungen an Library oder
Tests. Ein Cursor-Agent mit **Composer 2.5** hat die Interfaces anschließend
unabhängig gelesen und die gemessenen Befunde anhand dieses Berichts überprüft.
Im read-only Ask-Modus führte Composer selbst keinen Checker aus. Seine Aussage,
der Source Tree enthalte keinen `py.typed`-Marker, widersprach dem Git-Inventar
und der Wheel-Inspektion und wurde zur gezielten Korrektur zurückgegeben.

## Messmethode

Pyright **1.1.414**, `typeCheckingMode=basic`, Zielversion Python **3.12**,
`extraPaths` auf `src/`, vorhandene `.venv` mit Python **3.13.5** zur
Dependency-Auflösung. Prüfung über öffentliche `from superjobs import ...`
Imports. Die Beispiele wurden statisch geprüft, nicht als Jobs ausgeführt.

Ein isoliertes Consumer-Beispiel enthielt vollständig definierte und optionale
Verträge, passende und unpassende Handler sowie `assert_type` und `reveal_type`.
Der Checker meldete **vier erwartbare Fehler**: zwei verlorene Outcome-Typen und
zwei falsche Request-/Event-Argumente. Der fehlerhafte Handler erzeugte **keinen**
Fehler. Es wurden weder globale Unterdrückungen noch `cast` verwendet.

Die schnelle Suite bestand mit **140 Tests**, die reale NATS-Suite mit **6 Tests**:

```powershell
.\.venv\Scripts\python.exe -m pytest -m 'not nats' -q -p no:cacheprovider
.\.venv\Scripts\python.exe -m pytest -m nats -q -rs -p no:cacheprovider
```

## Gemessene Ergebnisse

| Consumer-Ausdruck | Pyright-Ergebnis | Bedeutung |
| --- | --- | --- |
| `Job(..., request=Request, result=Result, event=Event)` | `Job[Request, Result, Event]` | Vollständiger Vertrag wird inferiert. |
| `Job(..., request=Request, result=Result)` | `Job[Request, Result, Unknown]` | Fehlender Event-Vertrag wird nicht zu `None`. |
| `Job(..., result=Result)` | `Job[Unknown, Result, Unknown]` | Request- und Event-Typ bleiben unbestimmt. |
| `Job("probe.noop")` | `Job[Unknown, Unknown, Unknown]` | No-op-Vertrag verliert sämtliche Payload-Typen. |
| `jobs.client(full)` | `JobClient[Request, Result, Event]` | Client erhält die vollständigen Typen. |
| `await client.submit(Request(...))` | `JobHandle[Request, Result, Event]` | Handle erhält die vollständigen Typen. |
| `await client.run(...)`, `await handle.result()`, `await handle` | `Result` | Result-Typ bleibt in dieser Pyright-Konfiguration erhalten. |
| `await handle.outcome()` | `JobSucceeded[Unknown] \| JobFailedOutcome \| JobCancelledOutcome` | Erfolgreiches Outcome verliert den Result-Typ. |
| `outcome.result` nach `isinstance(outcome, JobSucceeded)` | `Unknown` | Narrowing stellt den Result-Typ nicht wieder her. |
| `handle.events()` | Elemente `JobEvent[Event]` | Event-Hülle erhält den Vertragstyp. |
| Dekorierter, korrekt annotierter Handler | `(...) -> Any` | Argumente und Rückgabetyp der Funktion gehen verloren. |
| Falscher Handler mit `str`-Request/-Result und `JobContext[str]` | kein Fehler am Decorator | Vertrag und Handler sind statisch nicht verbunden. |
| `client.submit("wrong request")` | `reportArgumentType` | Falscher Request wird für vollständige Verträge erkannt. |
| `context: JobContext[Event]`; `context.emit("wrong event")` | `reportArgumentType` | Explizit typisierter Kontext erkennt falsche Events. |

## Reproduzierbarer Kern

Die folgenden Beispiele benötigen nur öffentliche Imports und einfache
Dataclasses; zum Prüfen genügt Pyright mit `src/` im Importpfad und den
Projektabhängigkeiten im ausgewählten Environment. Sie erzeugen absichtlich
Diagnostics und gehören deshalb nicht ungeprüft in eine positive Test-Suite.

```python
from dataclasses import dataclass
from typing import assert_type, reveal_type
from superjobs import (
    InMemoryTransport, Job, JobContext, JobOutcome, JobSucceeded, SuperJobs,
)

@dataclass
class Request:
    value: int

@dataclass
class Result:
    value: int

@dataclass
class Event:
    value: int

jobs = SuperJobs(transport=InMemoryTransport())
full = Job("probe.full", request=Request, result=Result, event=Event)
reveal_type(Job("probe.noevent", request=Request, result=Result))
reveal_type(Job("probe.norequest", result=Result))
reveal_type(Job("probe.noop"))

@jobs.handler(full)
async def wrong_handler(request: str, context: JobContext[str]) -> str:
    await context.emit("wrong event for full")
    return "wrong result for full"

reveal_type(wrong_handler)  # (...) -> Any; kein Vertragsfehler

async def consumer() -> None:
    client = jobs.client(full)
    handle = await client.submit(Request(value=1))
    assert_type(await client.run(Request(value=1)), Result)
    assert_type(await handle.result(), Result)
    assert_type(await handle, Result)
    outcome = await handle.outcome()
    assert_type(outcome, JobOutcome[Result])  # reportAssertTypeFailure
    if isinstance(outcome, JobSucceeded):
        assert_type(outcome.result, Result)  # reportAssertTypeFailure
    await client.submit("wrong request")  # reportArgumentType

async def context_negative(context: JobContext[Event]) -> None:
    await context.emit("wrong event")  # reportArgumentType
```

Beispielkonfiguration für eine temporäre Datei außerhalb des Repositories:

```json
{
  "include": ["typing_probe.py"],
  "extraPaths": ["ABSOLUTER_PROJEKTPFAD/src"],
  "venvPath": "ABSOLUTER_PROJEKTPFAD",
  "venv": ".venv",
  "pythonVersion": "3.12",
  "typeCheckingMode": "basic"
}
```

```powershell
uv tool run --from pyright==1.1.414 pyright --project PFAD/pyrightconfig.json
```

## Codebefunde und Grenzen

`SuperJobs.handler` nimmt `Job[Any, Any, Any]` an und liefert über einen
`Callable[..., Any]`-Decorator dieselbe breite Callable-Annotation zurück.
`_validate_callback` prüft zur Laufzeit nur eine Mindestzahl positionaler
Parameter, keine Übereinstimmung der Payload-Typen. Ein zukünftiges Interface
muss statische Handler-Kompatibilität und Laufzeit-Validierung getrennt zusagen.

`JobHandle.outcome` verwendet `JobOutcome` ohne Result-Typargument. `Job` besitzt
drei Typvariablen ohne Defaults; die optionalen Konstruktorargumente reichen in
den gemessenen Fällen nicht aus, ausgelassene Typargumente zu bestimmen.

`uv build --offline` erzeugte erfolgreich sdist und Wheel; eine ZIP-Inspektion
bestätigte `superjobs/py.typed` im Wheel. Diese Messung beweist noch
keinen Consumer-Import aus einem installierten Wheel, keine vollständige
Library-Typfehlerfreiheit und keine Übereinstimmung mit mypy oder strengeren
Checker-Modi. Die fehlende explizite Annotation von `__await__` ist ein möglicher
Portabilitätsbedarf; **ein Verlust beim direkten Await wurde hier nicht gemessen**.

Die 6 NATS-Tests verwenden mehrere Runtimes im selben Prozess. Sie belegen keine
Producer-/Worker-Prozesstrennung. Startup-Blöcke fangen beliebige Exceptions und
markieren sie als Skip; das kann Regressions verstecken. Es gibt noch keinen
CI-Workflow oder Checker-Eintrag in `pyproject.toml`.

## Vorschlag für die nächste Entscheidung

Zuerst anhand kleiner Consumer-Beispiele bestimmen, wie optionale Payloads
inferiert werden und welche Handler-Formen unterstützt werden. Daraus getrennte
Umsetzungsschritte für Typgarantien und Test-Infrastruktur ableiten. Positive
Beispiele müssen ihre Typen mit `assert_type` belegen; negative Beispiele müssen
die vorgesehenen Diagnostics auslösen. Checker-Abhängigkeiten, Wheel-Import und
eine verpflichtende NATS-CI-Prüfung sollten reproduzierbar werden.

Als kleinster möglicher Umsetzungsschritt bietet sich die Bindung von
`JobHandle.outcome()` an `JobOutcome[FinalT]` samt positiven Result-/Narrowing-
Prüfungen und negativen Request-/Event-Beispielen an. Die generische Union
`JobOutcome[Result]` ist bereits verwendbar; eine Neugestaltung des Alias ist
durch diese Messung nicht begründet. Konstruktor-Inferenz und Handler-Formen
bleiben eigene Interface-Entscheidungen. Lösungen für ausgelassene Payload-Typen
müssen die zugesagte Python-3.12-Unterstützung berücksichtigen. Inline
PEP-696-Defaults benötigen Python 3.13; `typing_extensions` ändert diese
Syntaxanforderung nicht. Eine etwaige Python-3.12-Lösung muss zum Beispiel auf
kompatiblen traditionellen TypeVar-/Generic-Formen oder Overloads beruhen.

Dies ist eine Empfehlung, keine bereits beschlossene Interface-Änderung.

Primärquellen zur Messmethode:
[Pyright: Typkonzepte und reveal_type](https://github.com/microsoft/pyright/blob/main/docs/type-concepts.md),
[Pyright: Konfiguration](https://github.com/microsoft/pyright/blob/main/docs/configuration.md),
[PEP 696: Defaults für Typparameter](https://peps.python.org/pep-0696/).

Der korrigierte Composer-Bericht bestätigte am Ende die Outcome-Typpropagation
als kleinsten Kandidaten und stellte klar, dass keine Neugestaltung des Alias
nötig ist. Seine Vorschläge für Ordnerlayout, Checker-Policy und spätere
Interface-Änderungen sind nicht vom Maintainer beschlossene Entscheidungen.
