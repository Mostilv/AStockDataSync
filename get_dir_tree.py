import os

def print_tree_folders_first(dir_path, level=0, is_last=False):
    ignore_dirs = {".git", "__pycache__"}
    ignore_files = {".DS_Store"}

    if not os.path.isdir(dir_path):
        return [f"{dir_path} is not a directory."]

    tree_structure = []
    prefix = ("│   " * (level - 1)) + ("└── " if is_last else "├── ")
    tree_structure.append(f"{prefix}{os.path.basename(dir_path)}")

    # Separate folders and files
    folders = []
    files = []
    for item in sorted(os.listdir(dir_path)):
        if item in ignore_dirs or item in ignore_files:
            continue
        item_path = os.path.join(dir_path, item)
        if os.path.isdir(item_path):
            folders.append(item)
        else:
            files.append(item)

    # Add folders first
    for idx, folder in enumerate(folders):
        folder_path = os.path.join(dir_path, folder)
        tree_structure.extend(print_tree_folders_first(folder_path, level + 1, idx == len(folders) - 1 and not files))

    # Add files later
    for idx, file in enumerate(files):
        file_prefix = "│   " * level + ("└── " if idx == len(files) - 1 else "├── ")
        tree_structure.append(f"{file_prefix}{file}")

    return tree_structure

# Get the current working directory and generate its tree with folders on top and .py files below
current_directory = os.getcwd()
tree = print_tree_folders_first(current_directory)

# Combine the lines and print the result
tree_output = "\n".join(tree)
print(tree_output)
