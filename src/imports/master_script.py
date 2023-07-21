import os, sys
import subprocess

def master_script(selected_files):
    current_directory = os.getcwd()
    files = os.listdir(current_directory)

    python_files = [file for file in files if file.endswith('.py')]

    for py_file in selected_files:
        if py_file in python_files:
            command = f'python "{py_file}"'
            subprocess.run(command, shell=True)
        else:
            print(f"the file '{py_file}' doesn't exist in the folder.")
            sys.exit(1)

if __name__ == "__main__":
    # specific files to execute
    selected_files_to_execute = ["import_adm1.py", "import_adm2.py","import_adm3.py","import_watershed.py","import_waterpoint.py","import_climatology.py"]

    master_script(selected_files_to_execute)
