import subprocess
from datetime import datetime
from pathlib import Path


def current_date():
    return datetime.now().strftime("%Y-%m-%d")


def get_project_root():
    path = Path(__file__).resolve().parents[2]
    return path


def get_convex_root():
    root = get_project_root()
    path = root / "convex"
    return path


def generate_export_file_name():
    date = current_date()
    file_name = f"convex-export-{date}.zip"
    return file_name


def generate_export_path():
    date = current_date()
    convex_root = get_convex_root()
    path = Path(f"{convex_root}/exports/{date}")
    return path


def directory_exists(folder):
    if folder.is_dir():
        return True
    else:
        return False


def create_directory():
    path = generate_export_path()
    if not directory_exists(path):
        path.mkdir(parents=True, exist_ok=True)


def build_full_path():
    folder = generate_export_path()
    file_name = generate_export_file_name()
    full_path = folder / file_name
    return full_path


def run_convex_export():
    create_directory()
    path = str(build_full_path())
    convex_root = get_convex_root()
    subprocess.run(
        ["npx", "convex", "export", "--path", path], check=True, cwd=convex_root
    )


if __name__ == "__main__":
    run_convex_export()
