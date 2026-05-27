# Agent instructions

-   Always run `pre-commit` before committing and pushing changes
-   To the best of your ability, ensure tests are passing
-   Follow assertion style (actual on left, expected on right)
-   Always mark AI-generated tests with `ai_generated` Pytest marker
-   Attempt to utilize `pytest.mark.parametrize` wherever appropriate to reduce duplication in test cases
-   For tests, avoid importing private-marked API functions (those with leading underscores) and always favor importing what is publicly exposed through `__init__.py` files
-   Bump the version in `pyproject.toml` once per pull request when either any file under `src/` changes (excluding `tests/` and `docs/`), or `pyproject.toml` itself changes.; do NOT bump for changes that are purely CI/workflow, documentation, or configuration (e.g., GitHub Actions workflows, `AGENTS.md`, `README.md` badges).
-   For API signatures, require keyword arguments for multi-input functions using `(*, ...)`. For any function with exactly one caller-supplied parameter (excluding `self` and `cls`), require positional-only usage with the `/` designator
-   Leave a short description of the change or addition in the top `## Upcoming` section of the `CHANGELOG.md` under the appropriate subsection (`### 🚀 Enhancement`, `### 🐛 Bug Fix`, `### 📝 Documentation`, `### 🔩 Dependency Updates`, or `### 🏠 Internal`) as a new item (line starts with `-`); create the subsection if it does not yet exist; include the GitHub PR link at the end of each entry in the format `([#N](https://github.com/stamped-principles/stamped-checklist/pull/N)`
-   PR titles should be human-readable and in the past tense; they should NOT use conventional commit style
-   Always add new imports to the top of the file rather than locally scoped inside a function; the only exception is if it is needed to avoid a circular dependency
-   Never include code other than imports, `__all__`, simple import errors, or magic `__dir__` overrides in any `__init__.py` file
-   For external dependencies, always avoid specific import style (e.g., using `import abc from xyz` keyword) in favor of the generic full import (e.g., `import xyz; xyz.abc`)
-   For internal imports, always use the relative import style (e.g., `from .foo import bar`); when monkeypatching such imports in tests, target the importing module's binding, not the original definition module (e.g., `foo.baz`, instead of `foo._bar.baz`)
-   Every commit you author MUST include a `Co-Authored-By` trailer identifying both your tool name + version and your underlying model + version. Format (replace all `<…>` placeholders with actual values): `Co-Authored-By: <Tool> <tool-version> / <Model> <model-version> <noreply@<vendor-domain>>
-   Avoid using excessive em-dashes, colons, and semi-colons in written text such as documentation. Prefer breaking into separate, shorter sentences instead.
