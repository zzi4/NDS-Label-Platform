"""
Step 2: 用 Claude Vision API 对每张图片打 ODD 标签，生成 auto_labeled.db

逻辑：
1. 读取 sessions_index.json（step1 生成）
2. 对每张图片调用 Claude Vision，返回结构化 ODD 标签 JSON
3. 一致性约束：同一 L2 地点的时间无关特征（道路类型/车道/几何等）取多数投票
4. 生成 SQLite DB，schema 与 labeling_data_v2.db 完全一致

运行：
  export ANTHROPIC_API_KEY=sk-ant-xxxx
  conda activate nds
  python step2_vlm_label.py
"""

import base64
import io
import json
import os
import re
import sqlite3
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import anthropic
from PIL import Image

INDEX_FILE = "/home/stu1/Projects/LabelWork/sessions_index.json"
OUTPUT_DB = "/home/stu1/Projects/LabelWork/auto_labeled.db"
CACHE_FILE = "/home/stu1/Projects/LabelWork/vlm_cache.json"  # 断点续跑缓存
# MODEL = "claude-opus-4-6"
MODEL = "claude-haiku-4-5-20251001"

WORKERS = 2          # 并发数
MAX_RPM = 40          # API 每分钟请求上限
MAX_IMAGES_PER_CALL = 9  # 每次 API 调用最多图片数（Claude 支持 20+，9 = 3天×3张）

# ─── 限流器 ──────────────────────────────────────────────────────────

class RateLimiter:
    """令牌桶限流：保证 API 请求均匀分布，不超过 MAX_RPM"""
    def __init__(self, max_per_minute: int):
        self._interval = 60.0 / max_per_minute
        self._lock = threading.Lock()
        self._last = 0.0

    def acquire(self):
        with self._lock:
            wait = self._interval - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()


# ─── 标签体系（与 label_st.py 保持一致）─────────────────────────────
TOP_LEVEL_CONFIG = {
    "区域": ["封闭园区", "交通管制区域", "开放道路"],
    "城市道路": ["快速路", "主干路", "次干路", "支路", "街巷"],
    "公路": ["高速公路", "一级公路", "二级公路", "三级公路", "四级公路"],
    "乡村道路": ["村道", "其他乡村内部道路"],
    "其他道路": ["厂矿", "林区", "港口", "专用道路"],
    "停车区域": ["室内停车场", "室外停车场", "路侧停车位"],
    "自动驾驶场景": ["封闭场景", "半封闭场景", "开放场景"],
}

LABEL_SCHEMA = {
    "一、道路静态环境": {
        "1.2 道路表面": {
            "表面类型": ["沥青", "混凝土", "土路", "碎石", "冰雪路面", "金属板"],
            "表面状态": ["干燥", "潮湿", "积水", "积雪", "结冰", "泥泞"],
        },
        "1.3 道路几何": {
            "坡度": ["平路", "上坡", "下坡", "起伏路"],
            "曲率": ["直线", "弯道 (曲率<0.01)", "弯道 (0.01<曲率<0.05)", "弯道 (曲率>0.05)"],
            "横坡": ["正常排水坡度", "反超高", "无横坡"],
        },
        "1.4 包含车道特征": {
            "最宽车道数量": ["单车道", "双车道", "三车道", "四车道及以上"],
            "车道类型": ["普通车道", "公交专用道", "HOV车道", "潮汐车道", "应急车道",
                        "非机动车道", "人行道", "汇入匝道", "汇出匝道"],
            "车道宽度": ["标准", "狭窄", "超宽"],
        },
        "1.5 道路边缘": {
            "边缘类型": ["路缘石", "护栏 (金属)", "护栏 (混凝土)", "草地/泥土", "无物理隔离"],
        },
        "1.6 道路交叉": {
            "交叉类型": ["路段 (无交叉)", "平面交叉 (十字)", "平面交叉 (丁字)",
                        "平面交叉 (畸形)", "大型环岛 (出入口数 > 4)", "小型环岛", "立体交叉"],
        },
    },
    "二、交通设施": {
        "2.1 交通控制": {
            "信号灯": ["有", "无"],
            "标志牌": ["限速", "禁止", "指示", "警告", "施工", "无"],
            "地面标签": ["实线", "虚线", "双黄线", "导流线", "斑马线", "标线磨损"],
        },
        "2.2 路侧与周边环境": {
            "设施": ["无", "路灯", "电线杆", "隔音墙", "路边树木", "路边停车位",
                    "地面停车场出入口", "隧道出入口", "居民楼", "商场", "学校",
                    "医院", "公园", "绿化带"],
        },
        "2.3 特殊设施": {
            "类型": ["收费站", "检查站", "施工区域围挡", "减速带", "无"],
        },
    },
    "三、动态目标 (路面状况)": {
        "3.1 机动车": {
            "类型": ["轿车", "客车/巴士", "卡车/货车", "特种车辆 (警)",
                    "特种车辆(消)", "特种车辆(救)", "工程车辆"],
        },
        "3.2 VRU": {
            "类型": ["自行车", "电动车", "三轮车", "行人", "无"],
        },
        "3.3 动物": {"类型": ["有", "无"]},
        "3.4 障碍物": {
            "类型": ["落石", "遗洒物", "倒伏树木", "锥桶", "无"],
        },
        "3.5 事故车辆": {"类型": ["有", "无"]},
    },
    "四、大气环境": {
        "4.1 天气": {
            "类型": ["晴", "多云", "阴", "雨 (小/中/大)", "雪", "雾", "冰雹"],
        },
        "4.2 颗粒物": {
            "类型": ["无", "雾霾", "沙尘", "烟尘"],
        },
        "4.3 光照": {
            "来源": ["自然光", "人工照明", "混合光"],
            "强度": ["正常", "强光/逆光", "弱光/昏暗", "黑暗"],
        },
        "4.4 气温": {
            "估算": ["极寒 (< -20℃)", "寒冷 (-20℃ ~ -10℃)", "舒适 (-10℃ ~ 10℃)",
                    "炎热 (10℃ ~ 20℃)", "极热 (> 20℃)"],
        },
    },
}

# 多选字段
MULTI_SELECT_FIELDS = {
    ("1.3 道路几何", "曲率"),
    ("1.4 包含车道特征", "车道类型"),
    ("1.4 包含车道特征", "车道宽度"),
    ("2.1 交通控制", "地面标签"),
    ("2.2 路侧与周边环境", "设施"),
    ("3.1 机动车", "类型"),
    ("3.2 VRU", "类型"),
    ("3.4 障碍物", "类型"),
    ("1.6 道路交叉", "交叉类型"),
}

# 时间无关字段（同一地点多时段应保持一致）
TIME_INDEPENDENT_PATHS = [
    ("一、道路静态环境", "1.2 道路表面", "表面类型"),
    ("一、道路静态环境", "1.3 道路几何", "坡度"),
    ("一、道路静态环境", "1.3 道路几何", "曲率"),
    ("一、道路静态环境", "1.3 道路几何", "横坡"),
    ("一、道路静态环境", "1.4 包含车道特征", "最宽车道数量"),
    ("一、道路静态环境", "1.4 包含车道特征", "车道类型"),
    ("一、道路静态环境", "1.4 包含车道特征", "车道宽度"),
    ("一、道路静态环境", "1.5 道路边缘", "边缘类型"),
    ("一、道路静态环境", "1.6 道路交叉", "交叉类型"),
    ("二、交通设施", "2.1 交通控制", "信号灯"),
    ("二、交通设施", "2.1 交通控制", "标志牌"),
    ("二、交通设施", "2.2 路侧与周边环境", "设施"),
    ("二、交通设施", "2.3 特殊设施", "类型"),
]


# ─── VLM 调用 ────────────────────────────────────────────────────────

def build_prompt() -> str:
    """构建 VLM 提示词"""
    schema_str = json.dumps(LABEL_SCHEMA, ensure_ascii=False, indent=2)
    top_str = json.dumps(TOP_LEVEL_CONFIG, ensure_ascii=False, indent=2)
    return f"""你是一名自动驾驶数据标注专家。这是一张无人机航拍视角的道路图像。
请根据图像内容，按照以下标签体系输出 JSON 格式的 ODD（运行设计域）标注结果。

【顶层道路类型】（从下列选择一个 category 和一个 subcategory）：
{top_str}

【次级标签体系】（每个字段从给定选项中选择；标注了 [多选] 的字段返回列表，其余返回单个字符串）：
{schema_str}

多选字段（返回列表）：曲率、车道类型、车道宽度、地面标签、设施、3.1机动车类型、3.2VRU类型、3.4障碍物类型、交叉类型

注意事项：
1. 只从给定选项中选择，不要自造新标签
2. 无法判断的字段，选最接近的选项
3. 动态目标（第三大类）和大气环境（第四大类）根据图像当前状态标注，如看不到机动车则 3.1类型 为空列表[]
4. 从航拍视角推断道路几何特征
5. 气温根据植被、积雪、服装等视觉线索推断

请严格按如下 JSON 结构输出，不要有多余文字：
{{
  "top_road_category": "城市道路",
  "top_road_subcategory": "主干路",
  "tags": {{
    "一、道路静态环境": {{
      "1.2 道路表面": {{"表面类型": "沥青", "表面状态": "干燥"}},
      "1.3 道路几何": {{"坡度": "平路", "曲率": ["直线"], "横坡": "正常排水坡度"}},
      "1.4 包含车道特征": {{"最宽车道数量": "四车道及以上", "车道类型": ["普通车道"], "车道宽度": ["标准"]}},
      "1.5 道路边缘": {{"边缘类型": "路缘石"}},
      "1.6 道路交叉": {{"交叉类型": ["路段 (无交叉)"]}}
    }},
    "二、交通设施": {{
      "2.1 交通控制": {{"信号灯": "有", "标志牌": "无", "地面标签": ["实线", "虚线"]}},
      "2.2 路侧与周边环境": {{"设施": ["路灯", "绿化带"]}},
      "2.3 特殊设施": {{"类型": "无"}}
    }},
    "三、动态目标 (路面状况)": {{
      "3.1 机动车": {{"类型": ["轿车"]}},
      "3.2 VRU": {{"类型": ["无"]}},
      "3.3 动物": {{"类型": "无"}},
      "3.4 障碍物": {{"类型": ["无"]}},
      "3.5 事故车辆": {{"类型": "无"}}
    }},
    "四、大气环境": {{
      "4.1 天气": {{"类型": "晴"}},
      "4.2 颗粒物": {{"类型": "无"}},
      "4.3 光照": {{"来源": "自然光", "强度": "正常"}},
      "4.4 气温": {{"估算": "舒适 (-10℃ ~ 10℃)"}}
    }}
  }}
}}"""


MAX_SIDE = 512  # 最长边像素上限（航拍道路标注 512px 已足够，减少约 75% 图片 token）


def _resize_image(image_path: str) -> tuple[bytes, str]:
    """读取并压缩图片到最长边 ≤ MAX_SIDE，统一输出 JPEG"""
    with Image.open(image_path) as img:
        w, h = img.size
        if max(w, h) > MAX_SIDE:
            scale = MAX_SIDE / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=85)
        return buf.getvalue(), "image/jpeg"


def build_prompt_batch(n: int) -> str:
    """构建多图批量标注 prompt，要求返回长度为 n 的 JSON 数组"""
    schema_str = json.dumps(LABEL_SCHEMA, ensure_ascii=False, indent=2)
    top_str = json.dumps(TOP_LEVEL_CONFIG, ensure_ascii=False, indent=2)
    example = json.dumps({
        "top_road_category": "城市道路",
        "top_road_subcategory": "主干路",
        "tags": {
            "一、道路静态环境": {
                "1.2 道路表面": {"表面类型": "沥青", "表面状态": "干燥"},
                "1.3 道路几何": {"坡度": "平路", "曲率": ["直线"], "横坡": "正常排水坡度"},
                "1.4 包含车道特征": {"最宽车道数量": "四车道及以上", "车道类型": ["普通车道"], "车道宽度": ["标准"]},
                "1.5 道路边缘": {"边缘类型": "路缘石"},
                "1.6 道路交叉": {"交叉类型": ["路段 (无交叉)"]}
            },
            "二、交通设施": {
                "2.1 交通控制": {"信号灯": "有", "标志牌": "无", "地面标签": ["实线"]},
                "2.2 路侧与周边环境": {"设施": ["路灯"]},
                "2.3 特殊设施": {"类型": "无"}
            },
            "三、动态目标 (路面状况)": {
                "3.1 机动车": {"类型": ["轿车"]},
                "3.2 VRU": {"类型": ["无"]},
                "3.3 动物": {"类型": "无"},
                "3.4 障碍物": {"类型": ["无"]},
                "3.5 事故车辆": {"类型": "无"}
            },
            "四、大气环境": {
                "4.1 天气": {"类型": "晴"},
                "4.2 颗粒物": {"类型": "无"},
                "4.3 光照": {"来源": "自然光", "强度": "正常"},
                "4.4 气温": {"估算": "舒适 (-10℃ ~ 10℃)"}
            }
        }
    }, ensure_ascii=False)

    return f"""你是一名自动驾驶数据标注专家。你将收到 {n} 张无人机航拍道路图像（按顺序标为图1~图{n}）。
请对每张图片分别标注，输出包含 {n} 个元素的 JSON 数组，顺序与图片一致。

【顶层道路类型】（每张图选一个 category 和一个 subcategory）：
{top_str}

【次级标签体系】（每个字段从给定选项中选择）：
{schema_str}

多选字段（返回列表）：曲率、车道类型、车道宽度、地面标签、设施、3.1机动车类型、3.2VRU类型、3.4障碍物类型、交叉类型

注意事项：
1. 只从给定选项中选择，不要自造新标签
2. 动态目标和大气环境根据各图当前状态独立标注
3. 从航拍视角推断道路几何特征

请严格按如下 JSON 数组格式输出，共 {n} 个元素，不要有多余文字：
[
  {example},
  ... 共 {n} 个
]"""


_rate_limiter = RateLimiter(MAX_RPM)


def call_vlm_batch(client: anthropic.Anthropic, sessions: list, prefix: str = "  ") -> list | None:
    """
    批量调用 Claude Vision，一次请求处理同一天的多张图片。
    sessions: 包含 image_path / collection_time 的 session 列表（1~3 个）
    返回与 sessions 等长的标注结果列表，失败返回 None。
    """
    n = len(sessions)
    prompt_text = build_prompt_batch(n)

    # 构造多图消息内容
    content = []
    for i, s in enumerate(sessions):
        content.append({"type": "text", "text": f"[图{i+1}] 采集时间: {s.get('collection_time', '')}"})
        raw_bytes, media_type = _resize_image(s["image_path"])
        content.append({"type": "image", "source": {
            "type": "base64",
            "media_type": media_type,
            "data": base64.standard_b64encode(raw_bytes).decode("utf-8"),
        }})
    content.append({"type": "text", "text": prompt_text})

    messages = [{"role": "user", "content": content}]

    for attempt in range(8):
        try:
            _rate_limiter.acquire()  # 限速：均匀分布请求，不超过 MAX_RPM
            response = client.messages.create(
                model=MODEL, max_tokens=2000 * n, messages=messages
            )
            text_block = next((blk for blk in response.content if blk.type == "text"), None)
            text = text_block.text.strip() if text_block else ""

            m = re.search(r"\[.*\]", text, re.DOTALL)
            if m:
                results = json.loads(m.group())
                if isinstance(results, list) and len(results) == n:
                    return results
                print(f"{prefix}[WARN] 数组长度 {len(results) if isinstance(results, list) else '?'} ≠ {n}，重试")
                time.sleep(2)
            else:
                print(f"{prefix}[WARN] 响应无 JSON 数组(尝试{attempt+1}): {text[:200]!r}")
                time.sleep(2)
        except json.JSONDecodeError as e:
            print(f"{prefix}[WARN] JSON解析失败(尝试{attempt+1}): {e}")
            time.sleep(2)
        except anthropic.RateLimitError:
            wait = min(30 * (2 ** attempt), 600)
            print(f"{prefix}[RATE LIMIT] 等待 {wait}s... (尝试{attempt+1}/8)")
            time.sleep(wait)
        except Exception as e:
            print(f"{prefix}[ERROR] API调用失败(尝试{attempt+1}): {e}")
            time.sleep(5)
    return None


# ─── 一致性约束 ───────────────────────────────────────────────────────

def get_field(tags: dict, section: str, sub: str, attr: str):
    """安全读取 tags 中的字段值"""
    try:
        return tags[section][sub][attr]
    except (KeyError, TypeError):
        return None


def set_field(tags: dict, section: str, sub: str, attr: str, value):
    """安全写入 tags 中的字段值"""
    tags.setdefault(section, {}).setdefault(sub, {})[attr] = value


def majority_vote(values: list):
    """对标签列表取多数投票。列表类型字段取并集中出现超过半数的选项"""
    if not values:
        return None
    # 列表字段：收集所有值，取出现次数超过 len(values)/2 的选项
    if isinstance(values[0], list):
        counter = Counter()
        for v in values:
            if isinstance(v, list):
                for item in v:
                    counter[item] += 1
        threshold = len(values) / 2
        result = [item for item, cnt in counter.items() if cnt >= threshold]
        # 若无超过一半的，取出现最多的
        if not result and counter:
            result = [counter.most_common(1)[0][0]]
        return result if result else values[0]
    else:
        return Counter(str(v) for v in values if v is not None).most_common(1)[0][0]


def enforce_consistency(labeled_sessions: list) -> list:
    """
    对同一 L2 地点的时间无关字段取多数投票，确保一致性
    同时保留时间相关字段的各自标注
    """
    # 按 L2 分组
    by_location = defaultdict(list)
    for s in labeled_sessions:
        by_location[s["l2"]].append(s)

    for l2, group in by_location.items():
        if len(group) <= 1:
            continue

        # 对每个时间无关字段取多数投票
        for (section, sub, attr) in TIME_INDEPENDENT_PATHS:
            values = []
            for s in group:
                v = get_field(s.get("tags", {}), section, sub, attr)
                if v is not None:
                    values.append(v)
            if not values:
                continue
            consensus = majority_vote(values)
            for s in group:
                set_field(s.setdefault("tags", {}), section, sub, attr, consensus)

        # top_road_category / top_road_subcategory 也取多数
        cats = [s.get("top_road_category") for s in group if s.get("top_road_category")]
        if cats:
            consensus_cat = Counter(cats).most_common(1)[0][0]
            subs = [s.get("top_road_subcategory") for s in group
                    if s.get("top_road_category") == consensus_cat and s.get("top_road_subcategory")]
            consensus_sub = Counter(subs).most_common(1)[0][0] if subs else ""
            for s in group:
                s["top_road_category"] = consensus_cat
                s["top_road_subcategory"] = consensus_sub

        print(f"  [一致性] {l2}: {len(group)} 个时段，道路类型统一为 {group[0].get('top_road_category')}/{group[0].get('top_road_subcategory')}")

    return labeled_sessions


# ─── 数据库写入 ───────────────────────────────────────────────────────

def init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dataset (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_name TEXT NOT NULL,
            folder_path TEXT NOT NULL,
            location_name TEXT,
            label_time TIMESTAMP,
            collection_time TEXT,
            top_road_category TEXT,
            top_road_subcategory TEXT,
            secondary_tags_json TEXT,
            has_dynamic_override INTEGER DEFAULT 0,
            has_atmosphere_override INTEGER DEFAULT 0,
            has_road_surface_override INTEGER DEFAULT 0,
            duration REAL,
            quality_tags TEXT,
            comments TEXT
        )
    """)
    conn.commit()
    return conn


def write_to_db(conn: sqlite3.Connection, session: dict):
    tags = session.get("tags", {})
    conn.execute("""
        INSERT INTO dataset
        (video_name, folder_path, location_name, label_time, collection_time,
         top_road_category, top_road_subcategory, secondary_tags_json,
         has_dynamic_override, has_atmosphere_override, has_road_surface_override,
         duration, quality_tags, comments)
        VALUES (?,?,?,?,?,?,?,?,0,0,0,?,?,?)
    """, (
        session["video_name"],
        session["folder_path"],
        session["location_name"],
        datetime.now().isoformat(),
        session.get("collection_time", ""),
        session.get("top_road_category", ""),
        session.get("top_road_subcategory", ""),
        json.dumps(tags, ensure_ascii=False),
        session.get("duration", 0),
        "",
        "",
    ))
    conn.commit()


# ─── 索引加载 ────────────────────────────────────────────────────────

def build_day_groups(index: dict) -> list:
    """
    按 day_summary 构建批处理组，每组包含同一天同一场景的 1~3 个代表 session。
    一组 = 一次 API 调用。
    """
    sessions_list = index.get("sessions", index) if isinstance(index, dict) else index
    day_summaries = index.get("day_summaries", []) if isinstance(index, dict) else []

    session_by_path = {s["folder_path"]: s for s in sessions_list}

    if not day_summaries:
        print("[WARN] 索引中无 day_summaries，每个 session 单独处理")
        return [{"key": s["folder_path"], "date_display": "", "reps": [s]} for s in sessions_list]

    groups = []
    seen = set()
    for day in day_summaries:
        reps = []
        for rep in day["representatives"]:
            fp = rep["folder_path"]
            if fp in seen:
                continue
            seen.add(fp)
            s = session_by_path.get(fp)
            if s:
                reps.append(s)
        if reps:
            groups.append({
                "key": f"{day.get('l1','')}/{day.get('l2','')}/{day.get('date','')}",
                "date_display": day.get("date_display", ""),
                "reps": reps,
            })
    return groups


def merge_into_batches(day_groups: list, max_images: int) -> list:
    """
    将 day groups 合并为更大批次，每批图片数 ≤ max_images。
    每批是一个 dict：{"sessions": [...], "label": "批次描述"}
    """
    batches, current_sessions, current_labels = [], [], []
    for g in day_groups:
        reps = g["reps"]
        if current_sessions and len(current_sessions) + len(reps) > max_images:
            batches.append({"sessions": current_sessions, "label": " / ".join(current_labels)})
            current_sessions, current_labels = [], []
        current_sessions.extend(reps)
        current_labels.append(g["date_display"] or g["key"].split("/")[-1])
    if current_sessions:
        batches.append({"sessions": current_sessions, "label": " / ".join(current_labels)})
    return batches


# ─── 主流程 ──────────────────────────────────────────────────────────

def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_AUTH_TOKEN")
    if not api_key:
        print("❌ 请先设置 ANTHROPIC_API_KEY 或 ANTHROPIC_AUTH_TOKEN 环境变量")
        return

    base_url = os.environ.get("ANTHROPIC_BASE_URL")
    client_kwargs = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url
        print(f"[API] 使用自定义端点: {base_url}")

    # 加载索引，构建批次（多个 day group 合并，每批最多 MAX_IMAGES_PER_CALL 张）
    with open(INDEX_FILE, encoding="utf-8") as f:
        index = json.load(f)
    day_groups = build_day_groups(index)
    batches = merge_into_batches(day_groups, MAX_IMAGES_PER_CALL)
    total_sessions = len(index.get("sessions", index)) if isinstance(index, dict) else len(index)
    n_reps = sum(len(g["reps"]) for g in day_groups)
    print(f"索引共 {total_sessions} 个 sessions，日代表图: {n_reps} 张 → {len(batches)} 个批次（每批≤{MAX_IMAGES_PER_CALL}张）")

    # 加载 VLM 缓存（支持断点续跑）
    cache = {}
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            cache = json.load(f)
        print(f"[断点续跑] 已缓存 {len(cache)} 条 VLM 结果")

    client = anthropic.Anthropic(**client_kwargs)
    cache_lock = threading.Lock()
    total = len(batches)
    print(f"[模式] 并行 {WORKERS} 线程，限速 {MAX_RPM} RPM")

    def process_batch(idx_batch):
        i, batch = idx_batch
        sessions = batch["sessions"]
        label = batch["label"]
        prefix = f"  [{i+1}/{total}] "

        # 检查哪些已缓存
        with cache_lock:
            uncached = [s for s in sessions if s["folder_path"] not in cache]
            cached_results = {s["folder_path"]: cache[s["folder_path"]]
                              for s in sessions if s["folder_path"] in cache}

        if not uncached:
            print(f"{prefix}[缓存] {label} ({len(sessions)} 张全部命中)")
        else:
            valid = [s for s in uncached if Path(s.get("image_path", "")).exists()]
            if len(uncached) > len(valid):
                print(f"{prefix}[WARN] {len(uncached)-len(valid)} 张图片不存在，跳过")
            if valid:
                print(f"{prefix}VLM 标注: {label} ({len(valid)} 张)")
                results = call_vlm_batch(client, valid, prefix=prefix + "  ")
                if results:
                    with cache_lock:
                        for s, r in zip(valid, results):
                            cache[s["folder_path"]] = r
                            cached_results[s["folder_path"]] = r
                    print(f"{prefix}✓ 完成")
                else:
                    print(f"{prefix}[FAIL] 标注失败，跳过")

        labeled_batch = []
        for s in sessions:
            result = cached_results.get(s["folder_path"])
            if result:
                sd = dict(s)
                sd["top_road_category"] = result.get("top_road_category", "")
                sd["top_road_subcategory"] = result.get("top_road_subcategory", "")
                sd["tags"] = result.get("tags", {})
                labeled_batch.append(sd)
        return labeled_batch

    # Step A: VLM 批量标注
    CACHE_SAVE_INTERVAL = 10
    labeled = []
    completed = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(process_batch, (i, b)): i
                   for i, b in enumerate(batches)}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                labeled.extend(res)
            completed += 1
            if completed % CACHE_SAVE_INTERVAL == 0:
                with cache_lock:
                    snapshot = dict(cache)
                with open(CACHE_FILE, "w", encoding="utf-8") as f:
                    json.dump(snapshot, f, ensure_ascii=False)

    # 最终保存缓存
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    print(f"\n✅ VLM 标注完成: {len(labeled)} 条")

    # Step B: 一致性约束
    print("\n[一致性约束处理中...]")
    labeled = enforce_consistency(labeled)

    # Step C: 写入 DB
    print(f"\n[写入数据库: {OUTPUT_DB}]")
    conn = init_db(OUTPUT_DB)
    # 清空旧数据
    conn.execute("DELETE FROM dataset")
    conn.commit()
    conn.execute("BEGIN")
    for s in labeled:
        write_to_db(conn, s)
    conn.commit()
    conn.close()

    print(f"✅ 数据库生成完成: {OUTPUT_DB}")
    print(f"   共 {len(labeled)} 条记录")

    # 统计
    by_cat = Counter(s.get("top_road_category", "未知") for s in labeled)
    print("\n道路类型分布：")
    for cat, cnt in by_cat.most_common():
        print(f"  {cat}: {cnt}")


if __name__ == "__main__":
    main()
