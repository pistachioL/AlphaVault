from __future__ import annotations

from pathlib import Path


def _python_files(root: Path) -> list[Path]:
    return [
        path
        for path in root.rglob("*.py")
        if "__pycache__" not in path.parts and ".venv" not in path.parts
    ]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_backend_must_not_import_reflex() -> None:
    root = Path(__file__).resolve().parents[1]
    backend_dir = root / "alphavault"
    violations: list[str] = []
    for path in _python_files(backend_dir):
        text = _read(path)
        if "from alphavault_reflex" in text or "import alphavault_reflex" in text:
            violations.append(str(path.relative_to(root)))
    assert violations == []


def test_reflex_must_not_import_legacy_ui_layer() -> None:
    root = Path(__file__).resolve().parents[1]
    reflex_dir = root / "alphavault_reflex"
    violations: list[str] = []
    for path in _python_files(reflex_dir):
        text = _read(path)
        if "from alphavault.ui" in text or "import alphavault.ui" in text:
            violations.append(str(path.relative_to(root)))
    assert violations == []


def test_reflex_must_not_depend_on_legacy_stock_objects_module() -> None:
    root = Path(__file__).resolve().parents[1]
    reflex_dir = root / "alphavault_reflex"
    violations: list[str] = []
    for path in _python_files(reflex_dir):
        text = _read(path)
        if (
            "from alphavault_reflex.services.stock_objects import" in text
            or "import alphavault_reflex.services.stock_objects" in text
        ):
            violations.append(str(path.relative_to(root)))
    assert violations == []
