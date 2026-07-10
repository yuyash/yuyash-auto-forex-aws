# AWS Package Guide

`aws` is the AutoForexV2 AWS adapter library.

## Responsibilities

- Own direct AWS integration code such as Athena-backed market data adapters.
- Adapt AWS service responses into `core` domain models before exposing them to
  strategies, notebooks, or server code.
- Keep AWS credentials, regions, database names, table names, and result
  locations configurable through environment-backed settings and explicit
  constructor arguments.

## Boundaries

- Do not place core trading strategy logic here; put it in `core` or strategy
  packages such as `snowball`.
- Do not expose web endpoints or gRPC services here.
- Do not let other packages call AWS services directly when this package can
  provide a focused adapter.

## Compatibility Policy

- Do not preserve backward compatibility in this package at this stage.
- Do not add compatibility aliases, deprecated wrappers, legacy shims, or
  duplicate old/new APIs.
- When an API changes, update all call sites and tests to the new API and remove
  the old implementation outright.

## Type Policy

- Prefer domain objects, enums, and structured models over accepting both an
  object and its serialized `str` form.
- Do not type public or internal APIs as `SomeObject | str` unless the function
  is explicitly a parser/factory at a serialization boundary, or the value is
  inherently textual such as an external ID, file path, protocol field, or log
  field.
- When removing `str` inputs, update all call sites and tests to construct the
  object before calling the API.

## Commit Policy

- Use Conventional Commits for all commits: `<type>(<scope>): <summary>`.
- Prefer the package name as the scope for package-local changes, for example
  `feat(aws): add athena tick source`.
- Use one of `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`,
  `build`, `ci`, `chore`, or `revert`.
- Keep summaries imperative, concise, and without a trailing period.
- For breaking changes, append `!` after the type/scope and include a
  `BREAKING CHANGE:` footer when more detail is needed.

## Commands

```bash
uv sync
uv run ruff check .
uv run ruff format .
uv run ty check
uv run pytest
```
