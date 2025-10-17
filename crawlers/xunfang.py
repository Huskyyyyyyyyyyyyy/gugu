import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# 自定义比赛名字 -> URL 映射字典
urls = {
    '神齐': 'http://www.benzinglive.cn/AP_M1/AP0000_list.aspx?pid=0133',
    '阳光': 'http://www.benzinglive.cn/AP_M1/AP0000_list.aspx?pid=0091',
    # 可继续添加更多比赛
    }


async def fetch_race_info(page, url):
    """
    异步请求网页并解析比赛信息
    :param page: Playwright 的 Page 对象
    :param url: 要请求的页面 URL
    :return: 包含比赛名称、首个 <td> 内容以及 URL 的字典
    """
    # 打开网页
    await page.goto(url)

    # 等待 JS 渲染完成，这里延迟 3 秒，可根据实际页面调整
    await page.wait_for_timeout(3000)

    # 获取渲染后的完整 HTML
    html = await page.content()

    # 使用 BeautifulSoup 解析 HTML
    soup = BeautifulSoup(html, 'html.parser')

    # 查找比赛名称 <span id="lab_racename">
    race_name_tag = soup.find('span', id='lab_racename')
    race_name = race_name_tag.text.strip() if race_name_tag else None


    # 查找第一个 <td align="center" style="width:80px;">
    td_tag = soup.find('td', align='center', style='width:80px;')
    first_td = td_tag.text.strip() if td_tag else None

    # 返回结果字典
    return {
        'race_name': race_name,
        'first_td': first_td,
        'url': url
        }


async def main():
    """
    主函数：循环解析多个 URL，存储结果
    """
    results = {}  # 用于存储所有比赛解析结果

    # 异步启动浏览器
    async with async_playwright() as p:
        # 启动 Chromium 浏览器，headless=True 表示无界面运行
        browser = await p.chromium.launch(headless=True)

        # 新建一个浏览器标签页
        page = await browser.new_page()

        # 循环处理字典中的每个 URL
        for name, url in urls.items():
            # 异步请求并解析
            info = await fetch_race_info(page, url)
            # 将结果存入字典，以自定义名字为 key
            results[name] = info

        # 关闭浏览器
        await browser.close()

    # 打印解析结果
    for name, data in results.items():
        print(f"{name}: {data}")


# 程序入口
if __name__ == '__main__':
    # asyncio.run 启动异步主函数
    asyncio.run(main())
