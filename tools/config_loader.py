import sys
import os
import yaml
import json

def load_config(section=None, file_path=None):
    """
    加载 YAML 配置文件，并返回指定部分配置
    :param section: 配置块名称，例如 'gongpeng'
    :param file_path: 配置文件相对路径
    """
    config_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_file = os.path.join(config_path, file_path)
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
        if section:
            return config[section]
        else:
            return config


if __name__ == '__main__':
    # 保证可以导入 gongpeng_crawler 模块
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from crawlers.gongpeng_crawler import GongpengCrawler

    # 1. 加载配置
    cfg = load_config(section='gongpeng', file_path='config/spider.yaml')
    print("✅ 配置加载成功：")
    print(yaml.dump(cfg, allow_unicode=True, sort_keys=False))
    print("=" * 80)

    # 2. 初始化爬虫
    crawler = GongpengCrawler(
        config_path='config/spider.yaml',
        config_section='gongpeng',
    )

    # 3. 抓取数据
    items = crawler.crawl_all()
    print(f"抓取到 {len(items)} 条数据\n{'-'*80}")

    # 4. 输出前几条对象（字典格式 + JSON 格式）
    for i, obj in enumerate(items[:5], start=1):
        # 转字典（GongpengInfo 通常会有 to_dict）
        if hasattr(obj, "to_dict"):
            data = obj.to_dict()
        elif hasattr(obj, "__dict__"):
            data = obj.__dict__
        else:
            data = obj

        # 打印普通字典
        print(f"[{i}] 原始对象：")
        print(data)

        # 打印 JSON 格式
        print(f"[{i}] JSON：")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print("-" * 80)
