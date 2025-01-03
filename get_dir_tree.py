import os

def print_tree_folders_first(dir_path, level=0, is_last=False):
    ignore_dirs = {".git", "__pycache__"}
    ignore_files = {".DS_Store"}

    if not os.path.isdir(dir_path):
        return [f"{dir_path} is not a directory."]

    tree_structure = []
    
    # 只有在 level > 0 时才打印目录名称
    if level > 0:
        prefix = ("│   " * (level - 1)) + ("└── " if is_last else "├── ")
        tree_structure.append(f"{prefix}{os.path.basename(dir_path)}")

    # 区分目录和文件
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

    # 递归打印文件夹
    for idx, folder in enumerate(folders):
        folder_path = os.path.join(dir_path, folder)
        # 注意，这里如果没有文件时，对最后一个文件夹要 is_last=True
        tree_structure.extend(
            print_tree_folders_first(
                folder_path,
                level + 1,
                is_last=(idx == len(folders) - 1 and not files)
            )
        )

    # 打印文件
    for idx, file in enumerate(files):
        file_prefix = ("│   " * level) + ("└── " if idx == len(files) - 1 else "├── ")
        tree_structure.append(f"{file_prefix}{file}")

    return tree_structure


if __name__ == "__main__":
    current_directory = os.getcwd()
    tree = print_tree_folders_first(current_directory)
    tree_output = "\n".join(tree)
    print(tree_output)

