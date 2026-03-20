import subprocess
from datetime import datetime
from pathlib import Path


def current_date() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_convex_root(project_root: Path) -> Path:
    return project_root / "convex"


def generate_export_file_name(date: str) -> str:
    return f"convex-export-{date}.zip"


def generate_export_path(convex_root: Path, date: str) -> Path:
    return convex_root / "exports" / date


def directory_exists(folder: Path) -> bool:
    return folder.is_dir()


def create_directory(path: Path) -> None:
    if not directory_exists(path):
        path.mkdir(parents=True, exist_ok=True)


def build_full_path(export_path: Path, file_name: str) -> Path:
    return export_path / file_name


def run_convex_export() -> str:
    date = current_date()
    project_root = get_project_root()
    convex_root = get_convex_root(project_root)
    export_path = generate_export_path(convex_root, date)
    file_name = generate_export_file_name(date)
    full_path = build_full_path(export_path, file_name)

    create_directory(export_path)

    cmd = ["npx", "convex", "export", "--path", str(full_path)]

    result = subprocess.run(
        cmd,
        cwd=str(convex_root),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Convex export failed\n"
            f"cwd: {project_root}\n"
            f"cmd: {cmd}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    return str(full_path)
