import subprocess
from datetime import datetime
from pathlib import Path


def current_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


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


def run_convex_export() -> None:
    date = current_date()
    project_root = get_project_root()
    convex_root = get_convex_root(project_root)
    export_path = generate_export_path(convex_root, date)
    file_name = generate_export_file_name(date)
    full_path = build_full_path(export_path, file_name)

    create_directory(export_path)
    subprocess.run(
        ["npx", "convex", "export", "--path", str(full_path)],
        check=True,
        cwd=convex_root,
    )


# if __name__ == "__main__":
#     run_convex_export()
