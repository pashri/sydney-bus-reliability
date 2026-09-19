"""Guard against pyarrow/pytz creeping back into the merger.

Local dev has both installed as dev dependencies (pyarrow for test
fixtures, pytz for fetching a TIMESTAMPTZ column back in the merge-SQL
tests), so a unit test run on a laptop cannot tell whether the merger
would actually import on Lambda, where neither is packaged. This test
statically walks the AST of the merger's own modules and everything
they import from ``src``, rather than running the code, so it catches
the divergence without needing a Lambda-shaped environment.
"""

import ast
from pathlib import Path
from typing import Final

FORBIDDEN: Final[frozenset[str]] = frozenset({'pyarrow', 'pytz'})
SRC_ROOT: Final[Path] = Path(__file__).resolve().parents[2] / 'src'
MERGER_MODULES: Final[tuple[str, ...]] = (
    'src.merger.handler',
    'src.merger.merge_sql',
)


def module_path(*, module: str) -> Path:
    """Resolve a dotted ``src.*`` module name to its source file.

    Parameters
    ----------
    module : str
        Dotted module name, e.g. ``src.merger.handler``.

    Returns
    -------
    Path
        The module's source file.
    """
    return SRC_ROOT.joinpath(*module.split('.')[1:]).with_suffix('.py')


def imported_names(*, source: str) -> set[str]:
    """Collect every top-level module name a source file imports.

    Parameters
    ----------
    source : str
        Python source text.

    Returns
    -------
    set[str]
        Each ``import x`` or ``from x import y`` target's dotted name.
    """
    tree = ast.parse(source)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def transitive_src_imports(*, roots: tuple[str, ...]) -> set[str]:
    """Walk the ``src.*`` import graph reachable from a set of modules.

    Parameters
    ----------
    roots : tuple[str, ...]
        Dotted module names to start from.

    Returns
    -------
    set[str]
        Every top-level import name reachable from ``roots``, local or
        third-party.
    """
    seen: set[str] = set()
    pending = list(roots)
    all_imports: set[str] = set()
    while pending:
        module = pending.pop()
        if module in seen:
            continue
        seen.add(module)
        source = module_path(module=module).read_text()
        found = imported_names(source=source)
        all_imports.update(found)
        pending.extend(
            name for name in found
            if name.startswith('src.') and name not in seen
        )
    return all_imports


def test_merger_import_graph_excludes_pyarrow_and_pytz() -> None:
    """Neither pyarrow nor pytz may appear anywhere the merger imports.

    Both are dev-only dependencies, absent from the merger's Lambda
    package, which has DuckDB and nothing else. An import of either
    reaches Lambda's ``Runtime.ImportModuleError`` on every invocation.
    """
    imports = transitive_src_imports(roots=MERGER_MODULES)
    top_level = {name.split('.')[0] for name in imports}
    assert not (top_level & FORBIDDEN)
