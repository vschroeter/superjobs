# Contract convenience typing

Research for [Decide shared contract layout and handler context requirements](https://github.com/vschroeter/superjobs/issues/4), checked on 2026-09-29. These are specification findings and design proposals, not measured guarantees of the current library or a resolution of the issue.

## Request objects and constructor arguments

Runtime convenience is feasible: a client can construct the declared request model from keyword arguments, then follow the same validation and submission path as an explicitly supplied request. Strong static checking needs the constructor signature to remain available in the client type; the three ordinary type arguments in `Job[Request, Result, Event]` do not themselves retain a parameter specification. This conclusion follows from the separate roles of ordinary type variables and `ParamSpec`, rather than from a restriction on runtime model construction. [Typing generics specification](https://typing.python.org/en/latest/spec/generics.html#paramspec)

There is a standard route without generated stubs or a checker plugin: accept the request constructor as `Callable[P, Request]`, infer `P`, and retain it through the Job/client interface. The constructor specification explicitly demonstrates a class passed to a `Callable[P, R]` parameter producing its constructor signature. Its rules also cover constructor overloads and unusual metaclasses, so arbitrary classes deserve compatibility tests. Annotating a value only as `type[Request]` does not additionally preserve `P`. [Constructor-to-callable specification](https://typing.python.org/en/latest/spec/constructors.html#converting-a-constructor-to-callable)

An overloaded submission method can conceptually accept either a positional request object or the captured constructor arguments. Both overloads return the same typed handle. This is a feasible design direction, but it needs checker proof: overload overlap, absent requests, constructor defaults, and signature erasure through public exports are part of the acceptance criteria. Pyright recommends overloads for differing call forms and notes that signature transformations can lose signature assistance. [Pyright typed-library guidance](https://github.com/microsoft/pyright/blob/main/docs/typed-libraries.md#overloads)

`ParamSpec` preserves the entire constructor signature. It cannot generically turn positional-or-keyword parameters into keyword-only parameters: `P.args` and `P.kwargs` must occur together. Therefore, a wrapper typed with both may also statically accept positional constructor arguments. Requiring keyword-only request constructors, accepting constructor-compatible positional calls, or providing explicit per-job keyword signatures are distinct proposals. Keeping only `**kwargs: P.kwargs` is invalid under the specification. [PEP 612 components](https://peps.python.org/pep-0612/#the-components-of-a-paramspec)

## Submission options and TypedDict

Additional keyword-only execution options cannot be inserted between `*args: P.args` and `**kwargs: P.kwargs`. A constructor could already contain the same names. Feasible signature proposals are `client.with_options(...).submit(device_id="sensor-17")`, preserving `P` on a configured view, or `submit_with(options, /, *args: P.args, **kwargs: P.kwargs)`, prepending a positional-only options object. Neither reserves request-field keywords. Execution-specific options such as idempotency keys should be scoped to one submission, not silently retained as defaults for later submissions. [PEP 612 semantics](https://peps.python.org/pep-0612/#semantics)

`**kwargs: Unpack[ConcreteRequestFields]` can provide checked names, required keys, and field types when `ConcreteRequestFields` is a declared `TypedDict`. It cannot unpack an arbitrary dataclass or Pydantic model. A generic TypedDict can vary its value types while retaining declared keys; that does not provide an operator extracting the fields of an arbitrary request type or permit an ordinary TypeVar to stand for arbitrary keyword shapes. A concrete per-job TypedDict/wrapper or generated stub remains an alternative, with a second signature to maintain. [Callable kwargs specification](https://typing.python.org/en/latest/spec/callables.html#unpack-for-keyword-arguments), [Generic TypedDict syntax](https://typing.python.org/en/latest/spec/typeddict.html#class-based-syntax)

## Dataclasses and Pydantic

Dataclass and `dataclass_transform` support synthesizes constructors using model fields, defaults, keyword-only settings, and aliases. It does not independently synthesize an unrelated generic client's submission signature; that still requires retained constructor parameters or explicit signatures. [Dataclass typing specification](https://typing.python.org/en/latest/spec/dataclasses.html)

Pydantic documents Pyright/Pylance constructor support through `dataclass_transform`. Its checked constructor types can be narrower than runtime coercion accepts, and some default declarations have limitations. Capturing that signature means preserving the checker's view, not describing every valid raw validation input. Pydantic's mypy plugin has additional configuration-specific constructor behavior; it should not be assumed identical to Pyright. [Pydantic editor support](https://pydantic.dev/docs/validation/latest/integrations/dev-tools/visual_studio_code/), [Pydantic mypy integration](https://pydantic.dev/docs/validation/latest/integrations/dev-tools/mypy/)

## Synchronous convenience

`asyncio.run()` manages and closes a new loop and should ideally serve as the program entry point. `asyncio.Runner` supports multiple top-level calls sharing an embedded loop and context; closing it shuts down the loop. Neither entry point can run when another loop is running in the same thread. These facts favor a one-shot synchronous submit-and-wait helper for scripts, or a context-managed synchronous client owning one runner for repeated operations. [Python runners documentation](https://docs.python.org/3/library/asyncio-runner.html)

A reusable synchronous client also needs a decision about background I/O between calls. Owning one `Runner` keeps loop identity stable but does not continuously run it while ordinary synchronous code executes. A dedicated loop thread is a different, more complex proposal. A handle returned from a closed one-shot runtime should expose durable execution identity or be reopened in a new runtime rather than imply that the old live connection remains usable. These are lifecycle implications and proposals, not current transport measurements.

## Measured constructor-forwarding probe

Codex independently ran a bounded prototype authored by Cursor Composer 2.5 with Pyright 1.1.414, basic mode, Python 3.12 static target, and the existing project environment on 2026-09-29. The prototype retained a constructor as `Callable[P, R]` through `TypedJob[P, R]` and `JobClient[P, R]`. Its `submit` overloads accepted either `request: R` positionally or the paired `P.args`/`P.kwargs` constructor signature.

- Positive cases: zero diagnostics for explicit request objects and constructor keywords, with inferred return types asserted for two different dataclasses and a Pydantic model, including defaulted fields.
- Negative cases: incorrect field type, missing required field, unknown keyword, mixed object/keyword input, unrelated request object, and incorrect Pydantic field type were all diagnosed. An invalid fixed keyword-only `trace` parameter alongside `P.args`/`P.kwargs` was also diagnosed. These seven failing sites produced eight diagnostics because the unrelated request object produced both an overload mismatch and an argument error.

This was a standalone static prototype with `NotImplementedError` bodies, not a working submission implementation. For simplicity its return type was the request type; it did not prove final-result/event propagation, public SuperJobs imports, installed-wheel behavior, no-request jobs, arbitrary constructor overloads, or runtime validation. Mixed-input rejection was demonstrated for the tested constructors, not every possible constructor shape. Its results establish a feasible typing direction without settling the final interface.

The temporary probe files were removed after recording these results. A production proof must still cover the cases below.

## Proposed next proof

### Optional options-prefix probe

A second isolated Cursor Composer 2.5 prototype, independently checked by Codex with the same Pyright version and static target, tested a constructor overload `submit(options: SubmitOptions | None = None, /, *args: P.args, **kwargs: P.kwargs)` alongside an explicit-object overload `submit(request: R, /, *, options: SubmitOptions | None = None)`.

The final positive file had zero diagnostics for omitted options, a supplied positional options object, explicit request objects, defaulted fields, two keyword-only dataclasses, and a Pydantic model. A request with its own business field `options: str` also passed; positional-only execution options do not reserve that keyword. A concrete per-job signature supported `submit(device_id="...", options=SubmitOptions(...))` with an optional options keyword.

The negative file rejected empty request submissions (with and without a supplied options object), missing fields, incorrect fields/options, unknown keywords, mixed object/field input, and unrelated request objects. It also reproduced the invalid generic keyword-only options signature. The contrast `submit_with(options, /, *P.args, **P.kwargs)` rejected omission of its required options prefix. In total the final negative file produced 17 diagnostics and four reveal-type observations; its extra collision example intentionally used the already-invalid generic keyword-options prototype, so the count is not a library acceptance criterion.

These are static feasibility results using stub bodies, not installed SuperJobs behavior. The dataclass examples deliberately used keyword-only constructors. Required-field rejection was checked at direct negative call sites, not inferred from positive `assert_type` checks. The optional prefix is therefore a viable proposal for this checked scope; it is not an unconditional guarantee for every request constructor.

Use a bounded typing prototype to retain a constructor `ParamSpec` through a public Job/client, preserve explicit-object submission, and exercise dataclass and Pydantic requests. Check correct calls, missing/unknown keywords, incorrect values, defaults, field names matching execution-option names, absent requests, and installed-wheel imports. Include a negative test proving an explicit three-parameter Job annotation cannot silently erase the constructor signature. Decide whether keyword-only constructors or positional convenience are in scope before promising exact static support for the two submission forms.
