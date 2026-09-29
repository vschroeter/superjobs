# superjobs-contract-example

Minimal **contract package** for the `examples/contract_interface` iteration.

- Job definitions import `Job` only from the public `superjobs` package.
- Payload types are plain dataclasses; producers must not import worker code.

Install locally (with the parent `superjobs` tree present):

```powershell
uv pip install -e examples/contract_interface/superjobs_contract_example
```
