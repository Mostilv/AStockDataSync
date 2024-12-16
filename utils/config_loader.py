import yaml
import os

def load_config(file_path):
    """
    加载 YAML 配置文件。
    :param file_path: 配置文件路径。
    :return: 配置内容（字典形式）。
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"配置文件未找到: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

# 示例用法
if __name__ == "__main__":
    CONFIG_PATH = "config.yaml"

    try:
        config = load_config(CONFIG_PATH)
        print("配置加载成功:", config)
    except FileNotFoundError as e:
        print(f"配置错误: {e}")
