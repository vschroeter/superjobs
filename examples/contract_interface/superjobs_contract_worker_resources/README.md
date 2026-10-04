# Worker-only example dependency

This tiny distribution supplies a startup resource token used by the installed
worker example. Its separate module and distribution let verification prove that
the producer environment physically excludes worker dependencies. It has no
runtime dependencies and is not part of the SuperJobs library.

See [CLI application integration](../../../docs/design/cli-application.md).
