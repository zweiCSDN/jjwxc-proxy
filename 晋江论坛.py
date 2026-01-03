# -*- coding: utf-8 -*-
"""
JJWXC 收入金榜：按频道/标签分组抓取榜单 + 进入详情页补全信息，输出 JSON。

用法：
  python jjwxc_topten_scraper.py

可改参数：
  TOPTEN_URL：榜单地址
  MAX_BOOKS：限制抓取数量（None 为全量）
  SLEEP_SECONDS：每次请求间隔（建议 >= 1.0）
"""

from __future__ import annotations

import json
import random
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


TOPTEN_URL = "https://www.jjwxc.net/topten.php?orderstr=12&novelkind=9&month=0&opt=1"
BASE_URL = "https://www.jjwxc.net/"
MAX_BOOKS: Optional[int] = None         # None = 不限；也可设为 50/100 用于测试
SLEEP_SECONDS = 1.2                      # 温和抓取：建议 1~3 秒
TIMEOUT = 20


@dataclass
class ToptenItem:
    novelid: int
    title: str
    author: Optional[str]
    channel: str
    detail_url: str


@dataclass
class NovelDetail:
    article_type: Optional[str] = None   # 文章类型
    viewpoint: Optional[str] = None      # 作品视角
    word_count: Optional[int] = None     # 全文字数（尽量转 int）
    summary: Optional[str] = None        # 小说简介
    tags: Optional[List[str]] = None     # 内容标签（按空格/顿号等拆分）
    one_sentence: Optional[str] = None   # 一句话简介
    raw: Optional[Dict[str, str]] = None # 保留原始抓取到的字符串（便于排查）


def _new_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        # 尽量像正常浏览器访问；不要伪造过多“危险”头
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept-Language": "zh-CN,zh;q=0.9",
    })

    retry = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


def _sleep():
    # 加一点抖动，减少规律性
    time.sleep(SLEEP_SECONDS + random.uniform(0, 0.5))


def _detect_encoding(resp: requests.Response) -> str:
    # JJWXC 很多页面可能是 GBK；requests 有时能猜到，有时猜不到
    # 优先使用服务器声明，其次 apparent_encoding
    if resp.encoding and resp.encoding.lower() != "iso-8859-1":
        return resp.encoding
    if resp.apparent_encoding:
        return resp.apparent_encoding
    return "utf-8"


def fetch_html(sess: requests.Session, url: str) -> str:
    r = sess.get(url, timeout=TIMEOUT)
    enc = _detect_encoding(r)
    r.encoding = enc
    return r.text


def parse_novelid(href: str) -> Optional[int]:
    """
    从链接中提取 novelid。支持：
      onebook.php?novelid=9782753
      /onebook.php?novelid=9782753
      https://www.jjwxc.net/onebook.php?novelid=9782753
    """
    try:
        u = urlparse(href)
        qs = parse_qs(u.query)
        if "novelid" in qs and qs["novelid"]:
            return int(qs["novelid"][0])
    except Exception:
        pass

    m = re.search(r"novelid=(\d+)", href)
    if m:
        return int(m.group(1))
    return None


def normalize_channel_name(s: str) -> str:
    s = re.sub(r"\s+", "", s)
    s = s.replace("：", "").replace(":", "")
    return s or "未分类"


def guess_channel_for_link(a_tag) -> str:
    """
    由于无法保证页面结构稳定，这里用“向上/向前找标题节点”的启发式策略：
    - 优先找最近的 h1/h2/h3/h4/strong/b/td/th 等里较短的中文标题
    - 标题通常包含“言情/耽美/无CP/衍生/古代/现代”等关键词
    """
    KEYWORDS = ("言情", "耽美", "无CP", "衍生", "古代", "现代", "幻想", "悬疑", "科幻", "游戏", "轻小说", "影视", "综漫")

    # 往前找若干个“可能是标题”的节点
    for prev in a_tag.find_all_previous(["h1", "h2", "h3", "h4", "strong", "b", "td", "th", "font"], limit=80):
        t = prev.get_text(strip=True)
        if not t:
            continue
        t2 = normalize_channel_name(t)
        # 标题一般不会太长
        if 2 <= len(t2) <= 12 and any(k in t2 for k in KEYWORDS):
            return t2

    return "未分类"


def parse_topten(html: str) -> Dict[str, List[ToptenItem]]:
    soup = BeautifulSoup(html,   "html.parser")  # 或 "html.parser"

    KEYWORDS = ("言情", "纯爱", "百合", "无CP", "衍生", "二次元", "轻小说", "影视", "综漫", "古代", "现代", "幻想", "悬疑", "科幻", "游戏")

    def clean_channel(text: str) -> str:
        t = (text or "").strip()
        t = t.replace("：", "").replace(":", "")
        t = re.sub(r"\s+", "", t)
        return t or "未分类"

    def looks_like_channel(text: str) -> bool:
        t = clean_channel(text)
        if not t:
            return False
        # 标题一般不长，且包含关键词
        return 2 <= len(t) <= 20 and any(k in t for k in KEYWORDS)

    def find_channel_for_ul(ul: Tag) -> str:
        """
        为某个频道列表 ul.list_01 找到其“最近的频道标题”。
        常见标题位置：紧挨着 ul 之前的 h2/h3/div.title 等。
        """
        # 优先找常见的标题标签
        for prev in ul.find_all_previous(["h1", "h2", "h3", "h4", "h5", "h6"], limit=30):
            txt = prev.get_text(strip=True)
            if looks_like_channel(txt):
                return clean_channel(txt)

        # 再找可能的 div/span 标题（class/id 含 title/channel/category）
        for prev in ul.find_all_previous(["div", "span", "td", "th"], limit=60):
            cls = " ".join(prev.get("class", [])) if isinstance(prev, Tag) else ""
            pid = prev.get("id", "") if isinstance(prev, Tag) else ""
            txt = prev.get_text(strip=True)
            if ("title" in cls.lower() or "channel" in cls.lower() or "category" in cls.lower()
                or "title" in str(pid).lower() or "channel" in str(pid).lower() or "category" in str(pid).lower()):
                if looks_like_channel(txt):
                    return clean_channel(txt)

        # 兜底：只要像频道（短+关键词）也算
        for prev in ul.find_all_previous(True, limit=80):  # True = 任意标签
            if not isinstance(prev, Tag):
                continue
            txt = prev.get_text(strip=True)
            if looks_like_channel(txt):
                return clean_channel(txt)

        return "未分类"

    channel_book_map: Dict[str, List[ToptenItem]] = {}
    seen: set[int] = set()

    # 关键：取所有频道列表（不再只取第一个）
    uls = soup.select("ul.list_01")
    if not uls:
        # 兜底：如果 class 变了，仍然尝试从所有 onebook 链接解析
        uls = []

    for ul in uls:
        channel = find_channel_for_ul(ul)

        for li in ul.find_all("li", recursive=False) or ul.find_all("li"):
            # 小说链接（第一个 a：onebook.php?novelid=...）
            novel_a = li.select_one('a[href*="onebook.php"][href*="novelid="]')
            if not novel_a:
                continue

            href = novel_a.get("href", "")
            novelid = parse_novelid(href)
            if not novelid or novelid in seen:
                continue
            seen.add(novelid)

            title = novel_a.get_text(strip=True) or novel_a.get("title") or novel_a.get("alt") or ""
            if not title:
                continue

            # 作者（你提供的结构：<span class="author">女王不在家</span>）
            author = None
            author_span = li.select_one("span.author")
            if author_span:
                author = author_span.get_text(strip=True)

            if not author:
                # 兜底：作者链接文本
                author_a = li.select_one('a[href*="oneauthor.php"][href*="authorid="]')
                if author_a:
                    author = author_a.get_text(strip=True)

            item = ToptenItem(
                novelid=novelid,
                title=title,
                author=author,
                channel=channel,
                detail_url=urljoin(BASE_URL, f"onebook.php?novelid={novelid}")
            )
            channel_book_map.setdefault(channel, []).append(item)

    # 如果页面结构变化导致没抓到 ul.list_01，兜底从全页抓 onebook 链接（频道会是 未分类）
    if not channel_book_map:
        for a in soup.select('a[href*="onebook.php"][href*="novelid="]'):
            href = a.get("href", "")
            novelid = parse_novelid(href)
            if not novelid or novelid in seen:
                continue
            title = a.get_text(strip=True)
            if not title:
                continue
            seen.add(novelid)
            channel_book_map.setdefault("未分类", []).append(
                ToptenItem(
                    novelid=novelid,
                    title=title,
                    author=None,
                    channel="未分类",
                    detail_url=urljoin(BASE_URL, f"onebook.php?novelid={novelid}")
                )
            )

    return channel_book_map


def _extract_value_by_label(soup: BeautifulSoup, label: str) -> Optional[str]:
    """
    从详情页中按“标签名：值”提取值。尽量适配不同 DOM：
    - label 在 td/th，值在 next_sibling td
    - label 与值在同一行文本里（用正则切分）
    """
    # 1) DOM 结构化提取：找包含 label 的单元格
    node = soup.find(string=re.compile(re.escape(label)))
    if node and getattr(node, "parent", None):
        parent = node.parent
        # 常见：<td>文章类型：</td><td>原创-言情-古色古香</td>
        if parent.name in ("td", "th"):
            sib = parent.find_next_sibling("td")
            if sib:
                val = sib.get_text(" ", strip=True)
                return val or None

        # 有时 label 直接在一个元素里，值在下一个元素
        nxt = parent.find_next(["td", "span", "div"])
        if nxt:
            txt = nxt.get_text(" ", strip=True)
            # 避免返回自身或包含 label 的重复内容
            if txt and label not in txt:
                return txt

    # 2) 退化：整页文本按行匹配 “label：value”
    text = soup.get_text("\n", strip=True)
    for line in text.splitlines():
        if label in line:
            # 允许：label：value / label:value
            m = re.search(rf"{re.escape(label)}\s*[:：]\s*(.+)$", line)
            if m:
                v = m.group(1).strip()
                return v or None

    return None


def _extract_summary(soup: BeautifulSoup) -> Optional[str]:
    """
    小说简介位置在 JJWXC 可能有不同 id/class。
    这里做多策略：
    - 常见容器 id/class 选择器
    - 如果失败，退回用“小说简介：”标签方式提取
    """
    candidates = [
        "#novelintro",
        "#onebookintro",
        ".noveltext",
        ".novelintro",
        "div[style*='简介']",
    ]
    for sel in candidates:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text("\n", strip=True)
            if txt:
                return txt

    return _extract_value_by_label(soup, "小说简介")


def _parse_word_count(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    # 提取数字（可能带逗号/空格/“字”）
    m = re.search(r"([\d,，]+)", s)
    if not m:
        return None
    num = m.group(1).replace(",", "").replace("，", "")
    try:
        return int(num)
    except Exception:
        return None


def _split_tags(s: Optional[str]) -> Optional[List[str]]:
    """
    将“内容标签”字段尽量拆分成列表。
    兼容：空格/全角空格/换行/顿号/逗号/斜杠/竖线/分号/中点/·/&nbsp;
    """
    if not s:
        return None

    # 统一空白与常见分隔符
    s = s.replace("\xa0", " ")          # &nbsp;
    s = re.sub(r"\s+", " ", s).strip()

    # 有些页面会用多个符号混排做分隔
    parts = re.split(r"[ \u3000、，,;/；|｜/·•]+", s)
    tags = [p.strip() for p in parts if p and p.strip()]

    # 去重（保序）
    seen = set()
    uniq = []
    for t in tags:
        if t not in seen:
            uniq.append(t)
            seen.add(t)

    return uniq or None


def parse_detail(html: str) -> NovelDetail:
    soup = BeautifulSoup(html, "html.parser")

    article_type = _extract_value_by_label(soup, "文章类型")
    viewpoint = _extract_value_by_label(soup, "作品视角")
    word_count_raw = _extract_value_by_label(soup, "全文字数")
    tags_raw = _extract_value_by_label(soup, "内容标签")
    one_sentence = _extract_value_by_label(soup, "一句话简介")
    summary = _extract_summary(soup)

    return NovelDetail(
        article_type=article_type,
        viewpoint=viewpoint,
        word_count=_parse_word_count(word_count_raw),
        summary=summary,
        tags=_split_tags(tags_raw),
        one_sentence=one_sentence,
        raw={
            "全文字数_raw": word_count_raw or "",
            "内容标签_raw": tags_raw or "",
        }
    )

def build_dataset(sess: requests.Session, topten_url: str) -> Dict:
    html = fetch_html(sess, topten_url)
    _sleep()

    # 现在 parse_topten 返回的是 dict，直接使用
    channel_books_map = parse_topten(html)   # ← 这里不用改别的



    output_channels: Dict[str, List[Dict]] = {}
    total = sum(len(books) for books in channel_books_map.values())
    print(f"共识别到 {len(channel_books_map)} 个频道，共 {total} 本小说")

    for channel, books in channel_books_map.items():
        print(f"  → {channel}：{len(books)} 本")
        enriched = []
        for i, b in enumerate(books, 1):
            print(f"    [{i}/{len(books)}] 正在抓取 《{b.title}》...", end="")
            try:
                detail_html = fetch_html(sess, b.detail_url)
                detail = parse_detail(detail_html)
                _sleep()
                print("完成")
            except Exception as e:
                print("失败:", e)
                detail = NovelDetail()

            enriched.append({
                "novelid": b.novelid,
                "title": b.title,
                "author": b.author,
                "channel": b.channel,
                "detail_url": b.detail_url,
                "detail": asdict(detail),
            })
        output_channels[channel] = enriched

    data = {
        "source": {
            "topten_url": topten_url,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        },
        "channels": output_channels,
    }
    return data

def safe_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)  # Windows 非法字符
    name = re.sub(r"\s+", " ", name)
    return name

def main():
    sess = _new_session()
    data = build_dataset(sess, TOPTEN_URL)

    # ✅ 直接按日期生成文件夹（与 GitHub Actions 逻辑一致）
    date_str_for_file = datetime.now().strftime("%Y_%m_%d")  # 文件名用下划线
    date_str_for_folder = datetime.now().strftime("%Y-%m-%d")  # 文件夹用横杠（2026-01-03）
    date_str_for_meta = datetime.now().strftime("%Y/%m/%d")

    import os
    out_dir = os.path.join(os.getcwd(), date_str_for_folder)  # ← 关键：直接输出到日期文件夹
    os.makedirs(out_dir, exist_ok=True)

    # 1) 每个频道一个文件
    for channel, items in data["channels"].items():
        fname = f"{safe_filename(channel)}-{date_str_for_file}.json"
        path = os.path.join(out_dir, fname)

        payload = {
            "source": {
                **data["source"],
                "date": date_str_for_meta
            },
            "channel": channel,
            "count": len(items),
            "items": items
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        print(f"Saved channel file: {path}")

    # 2) 仍然输出一个总汇总文件（可选，建议保留）
    all_path = os.path.join(out_dir, f"ALL-{date_str_for_file}.json")
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved merged file: {all_path}")


if __name__ == "__main__":
    main()