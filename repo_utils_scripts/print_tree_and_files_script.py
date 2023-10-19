import subprocess
import os


def run_tree_command(
    output_file_path, exclude_patterns, repo_root, partially_exclude=[]
):
    additional_excludes = []
    for folder in partially_exclude:
        for root, dirs, files in os.walk(os.path.join(repo_root, folder)):
            additional_excludes.append("'" + root.replace(repo_root + "/", "") + "'")

    exclude_patterns = exclude_patterns.rstrip("'")
    exclude_patterns += "|" + "|".join(additional_excludes) + "'"

    command = f"tree {repo_root} -a -I {exclude_patterns}"
    try:
        with open(output_file_path, "w") as f:
            subprocess.run(command, stdout=f, shell=True)
    except Exception as e:
        print(f"An error occurred while running the tree command: {e}")


def append_file_content_to_output(
    output_file_path,
    exclude_folders,
    exclude_scripts_print_list,
    list_hide_code,
    repo_root,
    partially_exclude=[],
):
    for root, dirs, files in os.walk(repo_root):
        if any(exclude in root for exclude in exclude_folders):
            continue

        if any(
            root.startswith(os.path.join(repo_root, pe)) for pe in partially_exclude
        ):
            continue

        for file_name in files:
            if (
                (file_name.endswith(".py") or file_name.endswith(".yaml"))
                and (file_name not in exclude_scripts_print_list)
                and (file_name not in list_hide_code)
            ):
                print(file_name)
                print(exclude_scripts_print_list)
                file_path = os.path.join(root, file_name)
                try:
                    with open(file_path, "r") as src_file:
                        content = src_file.read()
                    with open(output_file_path, "a") as dest_file:
                        dest_file.write(f"\n\n=== Content of {file_path} ===\n\n")
                        dest_file.write(content)
                except Exception as e:
                    print(
                        f"An error occurred while reading/writing file {file_path}: {e}"
                    )
            elif file_name in list_hide_code:
                file_path = os.path.join(root, file_name)
                try:
                    with open(output_file_path, "a") as dest_file:
                        dest_file.write(f"\n\n=== Content of {file_path} ===\n\n")
                        dest_file.write(
                            "Code present but not reported for space reasons"
                        )
                except Exception as e:
                    print(
                        f"An error occurred while reading/writing file {file_path}: {e}"
                    )


if __name__ == "__main__":
    script_dir = os.path.dirname(
        os.path.abspath(__file__)
    )  # Get the directory of this script
    repo_root = os.path.abspath(
        os.path.join(script_dir, "..")
    )  # Go up two directories to get to the repo root

    output_file_path = os.path.join(repo_root, "custom_tree_and_files_corrected.txt")
    exclude_patterns = "'__pycache__|node_modules|public|.venv|.git|custom_tree_and_files_corrected.txt|data_prometheus|.vscode|remove|__init__.pyc|__py|__init__.py|__init__.py|repo_utils_scripts|notebooks|archive|experiments_storage'"
    exclude_folders = [
        "__pycache__",
        "node_modules",
        "public",
        ".venv",
        ".git",
        ".vscode",
        "repo_utils_scripts",
        "archive",
        "__init__.py",
        "notebooks",
        "investortoolkit",
        "backend",
        "mlruns",
        "experiments_storage",
        "data_prometheus",
    ]

    exclude_scripts_print_list = ["__init__.py"]
    list_hide_code = ["model_validation.py", "frontend"]

    partially_exclude = ["frontend", "backend"]

    run_tree_command(output_file_path, exclude_patterns, repo_root, partially_exclude)
    append_file_content_to_output(
        output_file_path,
        exclude_folders,
        exclude_scripts_print_list,
        list_hide_code,
        repo_root,
        partially_exclude,
    )
