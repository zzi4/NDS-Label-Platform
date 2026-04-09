"""
ODD 人工标注平台 v2
- 地点模式：静态字段（道路类型/几何/设施）只标一次，动态字段（天气/交通）逐图标注
- 数据来源：sessions_index.json → day_summaries（只标代表图）
- 展示每张图和每个地点的视频时长(duration)
- 新增地点：支持上传图片或输入路径，手动录入标签

运行：conda activate nds && streamlit run label_platform.py
"""
import json
import re
import sqlite3
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# ─── 常量 ─────────────────────────────────────────────────────────────
INDEX_FILE   = Path("/home/stu1/Projects/LabelWork/sessions_index.json")
DB_FILE      = "/home/stu1/Projects/LabelWork/label_platform.db"
UPLOAD_DIR   = Path("/home/stu1/Projects/LabelWork/uploaded_images")

CITY_MAP = {
    "ShenZhen": "深圳", "Changchun": "长春", "Hongkong": "香港",
    "Beijing": "北京", "Shanghai": "上海", "Guangzhou": "广州",
    "Haerbin": "哈尔滨", "Hangzhou": "杭州", "Wuhan": "武汉", "Chengdu": "成都",
}
BLUE = ["#1a56a8", "#059669", "#d97706", "#7c3aed",
        "#0891b2", "#dc2626", "#0d9488", "#f59e0b",
        "#6366f1", "#16a34a"]

# ─── 标签体系 ──────────────────────────────────────────────────────────
TOP_LEVEL_CONFIG = {
    "区域":        ["封闭园区","交通管制区域","开放道路"],
    "城市道路":    ["快速路","主干路","次干路","支路","街巷"],
    "公路":        ["高速公路","一级公路","二级公路","三级公路","四级公路"],
    "乡村道路":    ["村道","其他乡村内部道路"],
    "其他道路":    ["厂矿","林区","港口","专用道路"],
    "停车区域":    ["室内停车场","室外停车场","路侧停车位"],
    "自动驾驶场景": ["封闭场景","半封闭场景","开放场景"],
}

SECONDARY_MULTI = {
    "1.3 道路几何":      ["曲率"],
    "1.4 包含车道特征":  ["车道类型","车道宽度"],
    "1.6 道路交叉":      ["交叉类型"],
    "2.1 交通控制":      ["地面标签"],
    "2.2 路侧与周边环境": ["设施"],
    "3.1 机动车":        ["类型"],
    "3.2 VRU":           ["类型"],
    "3.4 障碍物":        ["类型"],
}

# 静态字段（道路本身特征，整个地点一致）
STATIC_SCHEMA = {
    "一、道路静态环境": {
        "1.2 道路表面": {
            "表面类型": ["沥青","混凝土","土路","碎石","冰雪路面","金属板"],
        },
        "1.3 道路几何": {
            "坡度": ["平路","上坡","下坡","起伏路"],
            "曲率": ["直线","弯道 (曲率<0.01)","弯道 (0.01<曲率<0.05)","弯道 (曲率>0.05)"],
            "横坡": ["正常排水坡度","反超高","无横坡"],
        },
        "1.4 包含车道特征": {
            "最宽车道数量": ["单车道","双车道","三车道","四车道及以上"],
            "车道类型": ["普通车道","公交专用道","HOV车道","潮汐车道","应急车道",
                        "非机动车道","人行道","汇入匝道","汇出匝道"],
            "车道宽度": ["标准","狭窄","超宽"],
        },
        "1.5 道路边缘": {
            "边缘类型": ["路缘石","护栏 (金属)","护栏 (混凝土)","草地/泥土","无物理隔离"],
        },
        "1.6 道路交叉": {
            "交叉类型": ["路段 (无交叉)","平面交叉 (十字)","平面交叉 (丁字)",
                        "平面交叉 (畸形)","大型环岛 (出入口数 > 4)","小型环岛","立体交叉"],
        },
    },
    "二、交通设施": {
        "2.1 交通控制": {
            "信号灯": ["有","无"],
            "标志牌": ["限速","禁止","指示","警告","施工","无"],
            "地面标签": ["实线","虚线","双黄线","导流线","斑马线","标线磨损"],
        },
        "2.2 路侧与周边环境": {
            "设施": ["无","路灯","电线杆","隔音墙","路边树木","路边停车位",
                    "地面停车场出入口","隧道出入口","居民楼","商场","学校",
                    "医院","公园","绿化带"],
        },
        "2.3 特殊设施": {"类型": ["收费站","检查站","施工区域围挡","减速带","无"]},
    },
}

# 动态字段（每张图独立标注）
DYNAMIC_SCHEMA = {
    "三、动态目标 (路面状况)": {
        "1.2 道路表面": {
            "表面状态": ["干燥","潮湿","积水","积雪","结冰","泥泞"],
        },
        "3.1 机动车":  {"类型": ["轿车","客车/巴士","卡车/货车","特种车辆 (警)","特种车辆(消)","特种车辆(救)","工程车辆"]},
        "3.2 VRU":    {"类型": ["自行车","电动车","三轮车","行人","无"]},
        "3.3 动物":   {"类型": ["有","无"]},
        "3.4 障碍物": {"类型": ["落石","遗洒物","倒伏树木","锥桶","无"]},
        "3.5 事故车辆": {"类型": ["有","无"]},
    },
    "四、大气环境": {
        "4.1 天气":  {"类型": ["晴","多云","阴","雨 (小/中/大)","雪","雾","冰雹"]},
        "4.2 颗粒物": {"类型": ["无","雾霾","沙尘","烟尘"]},
        "4.3 光照": {
            "强度": ["正常","强光/逆光","弱光/昏暗","黑暗"],
        },
    },
}

FULL_SCHEMA = {**STATIC_SCHEMA, **DYNAMIC_SCHEMA}

TAG_PATHS = [
    ("一、道路静态环境","1.2 道路表面","表面类型"),
    ("一、道路静态环境","1.3 道路几何","坡度"),
    ("一、道路静态环境","1.3 道路几何","曲率"),
    ("一、道路静态环境","1.4 包含车道特征","最宽车道数量"),
    ("一、道路静态环境","1.4 包含车道特征","车道类型"),
    ("一、道路静态环境","1.5 道路边缘","边缘类型"),
    ("一、道路静态环境","1.6 道路交叉","交叉类型"),
    ("二、交通设施","2.1 交通控制","信号灯"),
    ("二、交通设施","2.1 交通控制","地面标签"),
    ("二、交通设施","2.2 路侧与周边环境","设施"),
    ("二、交通设施","2.3 特殊设施","类型"),
    ("三、动态目标 (路面状况)","1.2 道路表面","表面状态"),
    ("三、动态目标 (路面状况)","3.1 机动车","类型"),
    ("三、动态目标 (路面状况)","3.2 VRU","类型"),
    ("三、动态目标 (路面状况)","3.3 动物","类型"),
    ("三、动态目标 (路面状况)","3.4 障碍物","类型"),
    ("三、动态目标 (路面状况)","3.5 事故车辆","类型"),
    ("四、大气环境","4.1 天气","类型"),
    ("四、大气环境","4.2 颗粒物","类型"),
    ("四、大气环境","4.3 光照","强度"),
]

# ─── 工具函数 ──────────────────────────────────────────────────────────
def extract_city(l1: str) -> str:
    m = re.search(r"\d{4}-([A-Za-z]+)-AerialVideo", l1)
    return CITY_MAP.get(m.group(1), m.group(1)) if m else l1

def parse_time(name: str) -> str:
    m = re.search(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})", name)
    if m:
        y, mo, d, h, mi = m.groups()
        return f"{y}年{int(mo)}月{int(d)}日 {int(h):02d}:{int(mi):02d}"
    return ""

def parse_hour(ct: str) -> int:
    m = re.search(r"\s(\d{1,2}):", ct or "")
    return int(m.group(1)) if m else -1

def time_period(h: int) -> str:
    if 7 <= h < 9:  return "早高峰 (07-09)"
    if 17 <= h < 19: return "晚高峰 (17-19)"
    if h >= 19:     return "夜间 (19+)"
    if h >= 0:      return "日常规"
    return "日常规"

def fmt_dur(minutes: float) -> str:
    if minutes >= 60:
        return f"{minutes/60:.1f}h"
    return f"{minutes:.0f}min"

def extract_tag(d: dict, sec: str, sub: str, attr: str):
    try:
        v = d[sec][sub][attr]
        return v if isinstance(v, list) else ([v] if v else None)
    except (KeyError, TypeError):
        return None

# ─── 索引加载 ──────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_location_groups() -> dict:
    """
    读取 sessions_index.json → day_summaries，按 l2 分组。
    返回 dict: l2 → {city, l1, total_duration, days, all_reps}
    """
    if not INDEX_FILE.exists():
        return {}
    with open(INDEX_FILE, encoding="utf-8") as f:
        idx = json.load(f)
    day_summaries = idx.get("day_summaries", [])

    groups: dict = {}
    for ds in day_summaries:
        l2 = ds["l2"]
        l1 = ds.get("l1", "")
        if l2 not in groups:
            groups[l2] = {
                "l2": l2, "l1": l1,
                "city": extract_city(l1),
                "total_duration": 0.0,
                "days": [],
                "all_reps": [],
            }
        groups[l2]["total_duration"] += ds.get("total_duration", 0)
        reps = ds.get("representatives", [])
        groups[l2]["days"].append({
            "date":           ds.get("date", ""),
            "date_display":   ds.get("date_display", ""),
            "total_duration": ds.get("total_duration", 0),
            "session_count":  ds.get("session_count", 0),
            "reps":           reps,
        })
        groups[l2]["all_reps"].extend(reps)

    # 按日期排序每个地点的 days
    for loc in groups.values():
        loc["days"].sort(key=lambda d: d["date"])

    return groups

# ─── 数据库 ────────────────────────────────────────────────────────────
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dataset (
                id                        INTEGER PRIMARY KEY AUTOINCREMENT,
                video_name                TEXT NOT NULL,
                folder_path               TEXT NOT NULL UNIQUE,
                location_name             TEXT,
                label_time                TIMESTAMP,
                collection_time           TEXT,
                top_road_category         TEXT,
                top_road_subcategory      TEXT,
                secondary_tags_json       TEXT,
                has_dynamic_override      INTEGER DEFAULT 0,
                has_atmosphere_override   INTEGER DEFAULT 0,
                has_road_surface_override INTEGER DEFAULT 0,
                duration                  REAL DEFAULT 0,
                quality_tags              TEXT DEFAULT '',
                comments                  TEXT DEFAULT '',
                image_path                TEXT DEFAULT ''
            )
        """)

@st.cache_data(ttl=30)
def load_labeled_fps() -> set:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            return {r[0] for r in conn.execute("SELECT folder_path FROM dataset").fetchall()}
    except Exception:
        return set()

def get_location_static(l2: str) -> dict:
    """从 DB 中读取该地点任意一行的静态标签，用于预填充"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute(
                "SELECT top_road_category, top_road_subcategory, secondary_tags_json "
                "FROM dataset WHERE location_name=? LIMIT 1", (l2,)
            ).fetchone()
        if row:
            tags = json.loads(row[2]) if row[2] else {}
            static_sec = {k: tags[k] for k in STATIC_SCHEMA if k in tags}
            return {"top_cat": row[0] or "", "top_sub": row[1] or "", "sections": static_sec}
    except Exception:
        pass
    return {}

def get_rep_dynamic(folder_path: str) -> dict:
    """读取某张代表图已有的动态标签"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute(
                "SELECT secondary_tags_json FROM dataset WHERE folder_path=?", (folder_path,)
            ).fetchone()
        if row and row[0]:
            tags = json.loads(row[0])
            return {k: tags[k] for k in DYNAMIC_SCHEMA if k in tags}
    except Exception:
        pass
    return {}

def _upsert_rep(conn, rep: dict, l2: str, top_cat: str, top_sub: str, merged_tags: dict, comments: str = ""):
    conn.execute("""
        INSERT OR REPLACE INTO dataset
        (video_name, folder_path, location_name, label_time, collection_time,
         top_road_category, top_road_subcategory, secondary_tags_json,
         has_dynamic_override, has_atmosphere_override, has_road_surface_override,
         duration, quality_tags, comments, image_path)
        VALUES (?,?,?,?,?,?,?,?,0,0,0,?,'',?,?)
    """, (
        Path(rep["image_path"]).name if rep.get("image_path") else "",
        rep["folder_path"],
        l2,
        datetime.now().isoformat(),
        rep.get("collection_time", ""),
        top_cat, top_sub,
        json.dumps(merged_tags, ensure_ascii=False),
        rep.get("duration", 0),
        comments,
        rep.get("image_path", ""),
    ))

def save_static_to_location(l2: str, loc: dict, top_cat: str, top_sub: str, static_sections: dict):
    """将静态标签保存到该地点所有代表图（保留已有动态标签）"""
    with sqlite3.connect(DB_FILE) as conn:
        for rep in loc["all_reps"]:
            # 保留现有动态标签
            existing_dyn = get_rep_dynamic(rep["folder_path"])
            merged = {**static_sections, **existing_dyn}
            _upsert_rep(conn, rep, l2, top_cat, top_sub, merged)

def save_dynamic_for_rep(rep: dict, l2: str, loc_static: dict, dynamic_sections: dict, comments: str = ""):
    """保存单张代表图的动态标签，合并地点静态标签"""
    top_cat = loc_static.get("top_cat", "")
    top_sub = loc_static.get("top_sub", "")
    static_sec = loc_static.get("sections", {})
    merged = {**static_sec, **dynamic_sections}
    with sqlite3.connect(DB_FILE) as conn:
        _upsert_rep(conn, rep, l2, top_cat, top_sub, merged, comments)

def save_manual_location(folder_path: str, video_name: str, location_name: str,
                         collection_time: str, top_cat: str, top_sub: str,
                         all_tags: dict, duration: float, image_path: str, comments: str):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO dataset
            (video_name, folder_path, location_name, label_time, collection_time,
             top_road_category, top_road_subcategory, secondary_tags_json,
             has_dynamic_override, has_atmosphere_override, has_road_surface_override,
             duration, quality_tags, comments, image_path)
            VALUES (?,?,?,?,?,?,?,?,0,0,0,?,'',?,?)
        """, (
            video_name, folder_path, location_name,
            datetime.now().isoformat(), collection_time,
            top_cat, top_sub,
            json.dumps(all_tags, ensure_ascii=False),
            duration, comments, image_path,
        ))

@st.cache_data(ttl=60)
def load_df() -> pd.DataFrame:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            df = pd.read_sql(
                "SELECT id,video_name,folder_path,location_name,collection_time,"
                "top_road_category,top_road_subcategory,secondary_tags_json,duration,image_path "
                "FROM dataset", conn
            )
    except Exception:
        return pd.DataFrame()
    df["city"] = df["folder_path"].apply(lambda p: extract_city(p.split("/")[0]) if p and "/" in p else "手动录入")
    df["year"] = df["folder_path"].apply(lambda p: p[:4] if p and p[:4].isdigit() else "")
    df["_tags"] = df["secondary_tags_json"].apply(
        lambda s: json.loads(s) if pd.notna(s) and s else {}
    )
    return df

# ─── 表单渲染 ──────────────────────────────────────────────────────────
def _field(schema_dict: dict, sec_key: str, sub_key: str, attr: str,
           options: list, existing: dict, form_key: str):
    """渲染单个字段（单选或多选）"""
    is_multi = attr in SECONDARY_MULTI.get(sub_key, [])
    ex_val = existing.get(sec_key, {}).get(sub_key, {}).get(attr)
    label = f"{sub_key} · {attr}"
    if is_multi:
        if isinstance(ex_val, str): ex_val = [ex_val] if ex_val else []
        elif not isinstance(ex_val, list): ex_val = []
        return st.multiselect(label, options, default=[v for v in ex_val if v in options], key=form_key)
    else:
        if isinstance(ex_val, list): ex_val = ex_val[0] if ex_val else ""
        return st.selectbox(label, options, index=options.index(ex_val) if ex_val in options else 0, key=form_key)

def render_schema_section(schema: dict, existing: dict, key_prefix: str) -> dict:
    """渲染 schema 所有字段（3列布局），返回收集到的标签 dict"""
    result = {}
    for sec, sub_dict in schema.items():
        st.markdown(f"<div class='sec-bar'>{sec}</div>", unsafe_allow_html=True)
        result[sec] = {}
        cols = st.columns(3)
        ci = 0
        for sub_key, attrs in sub_dict.items():
            result[sec][sub_key] = {}
            for attr, options in attrs.items():
                fk = f"{key_prefix}_{sec}_{sub_key}_{attr}"
                with cols[ci % 3]:
                    result[sec][sub_key][attr] = _field(
                        schema, sec, sub_key, attr, options,
                        existing.get(sec, {}), fk
                    )
                ci += 1
    return result

def render_schema_flat(schema: dict, existing: dict, key_prefix: str) -> dict:
    """渲染 schema 所有字段（垂直单列，适合窄列），返回收集到的标签 dict"""
    result = {}
    for sec, sub_dict in schema.items():
        st.markdown(f"<div class='sec-bar' style='font-size:11px'>{sec}</div>", unsafe_allow_html=True)
        result[sec] = {}
        for sub_key, attrs in sub_dict.items():
            result[sec][sub_key] = {}
            for attr, options in attrs.items():
                fk = f"{key_prefix}_{sec}_{sub_key}_{attr}"
                result[sec][sub_key][attr] = _field(
                    schema, sec, sub_key, attr, options,
                    existing.get(sec, {}), fk
                )
    return result

# ─── 页面1：地点标注 ───────────────────────────────────────────────────
def page_label():
    loc_groups = load_location_groups()
    if not loc_groups:
        st.error(f"未找到索引文件：{INDEX_FILE}")
        return

    labeled_fps = load_labeled_fps()

    # 侧边栏：地点列表
    with st.sidebar:
        st.markdown("### 选择地点")
        all_cities = sorted({v["city"] for v in loc_groups.values()})
        sel_city = st.multiselect("城市过滤", all_cities, default=all_cities, key="lbl_city")

        st.markdown("---")
        filtered_locs = {l2: loc for l2, loc in sorted(loc_groups.items())
                         if loc["city"] in sel_city}

        total_reps   = sum(len(loc["all_reps"]) for loc in filtered_locs.values())
        labeled_reps = sum(1 for loc in filtered_locs.values()
                           for rep in loc["all_reps"] if rep["folder_path"] in labeled_fps)
        st.markdown(f"**进度：{labeled_reps} / {total_reps} 张**")
        st.progress(labeled_reps / total_reps if total_reps else 0)
        st.markdown("---")

        for l2, loc in filtered_locs.items():
            n_rep   = len(loc["all_reps"])
            n_done  = sum(1 for r in loc["all_reps"] if r["folder_path"] in labeled_fps)
            if n_done == 0:     dot, clr = "⬜", "#9E9E9E"
            elif n_done < n_rep: dot, clr = "🔶", "#F57C00"
            else:               dot, clr = "✅", "#2E7D32"
            if st.button(
                f"{dot} {loc['city']} · {l2}  ({n_done}/{n_rep})",
                key=f"loc_{l2}", use_container_width=True
            ):
                st.session_state.sel_loc = l2

    if "sel_loc" not in st.session_state:
        st.info("← 从左侧选择一个地点开始标注")
        return

    l2  = st.session_state.sel_loc
    loc = loc_groups.get(l2)
    if not loc:
        st.warning("地点数据不存在"); return

    # 地点头部
    n_rep  = len(loc["all_reps"])
    n_done = sum(1 for r in loc["all_reps"] if r["folder_path"] in labeled_fps)
    st.markdown(
        f"<div class='loc-header'>"
        f"<span class='loc-city'>{loc['city']}</span>"
        f"<span class='loc-name'>{l2}</span>"
        f"<span class='loc-stat'>📅 {len(loc['days'])} 天 &nbsp;|&nbsp; "
        f"🖼 {n_rep} 张代表图 &nbsp;|&nbsp; "
        f"⏱ {fmt_dur(loc['total_duration'])} 总时长 &nbsp;|&nbsp; "
        f"{'✅' if n_done==n_rep else '🔶'} {n_done}/{n_rep} 已标注</span>"
        f"</div>", unsafe_allow_html=True
    )

    # Tab：静态 / 动态
    tab_static, tab_dynamic = st.tabs(["📍 静态标签（整个地点）", "🎬 动态标签（逐图）"])

    # ── Tab1：静态标签 ──────────────────────────────────────────────
    with tab_static:
        st.markdown(
            "<div class='tip'>道路几何、车道、设施等物理特征不随时间变化，"
            "填写一次后保存到本地点所有代表图。</div>", unsafe_allow_html=True
        )
        existing_static = get_location_static(l2)
        ex_sec = existing_static.get("sections", {})
        ex_cat = existing_static.get("top_cat", "")
        ex_sub = existing_static.get("top_sub", "")

        cats = list(TOP_LEVEL_CONFIG.keys())
        with st.form(key=f"static_{l2}"):
            c1, c2 = st.columns(2)
            with c1:
                top_cat = st.selectbox("道路主类", cats,
                    index=cats.index(ex_cat) if ex_cat in cats else 0,
                    key=f"scat_{l2}")
            with c2:
                subs = TOP_LEVEL_CONFIG[top_cat]
                top_sub = st.selectbox("道路子类", subs,
                    index=subs.index(ex_sub) if ex_sub in subs else 0,
                    key=f"ssub_{l2}")

            static_tags = render_schema_section(STATIC_SCHEMA, ex_sec, f"st_{l2}")
            submitted = st.form_submit_button(
                f"💾 保存静态标签 → 应用到本地点全部 {n_rep} 张代表图",
                type="primary", use_container_width=True
            )
        if submitted:
            save_static_to_location(l2, loc, top_cat, top_sub, static_tags)
            load_labeled_fps.clear(); load_df.clear()
            st.success(f"✅ 静态标签已保存并应用到 {n_rep} 张图！"); st.rerun()

    # ── Tab2：动态标签（逐图） ──────────────────────────────────────
    with tab_dynamic:
        st.markdown(
            "<div class='tip'>天气、光照、车辆等随时间变化，每张图独立标注。"
            "静态标签会自动沿用（请先完成静态标签）。</div>", unsafe_allow_html=True
        )
        loc_static = get_location_static(l2)

        for day in loc["days"]:
            dday  = day["date_display"]
            ddur  = fmt_dur(day["total_duration"])
            dreps = day["reps"]
            n_day_done = sum(1 for r in dreps if r["folder_path"] in labeled_fps)
            badge = "✅" if n_day_done == len(dreps) else ("🔶" if n_day_done > 0 else "⬜")

            with st.expander(
                f"{badge}  {dday}  ·  {ddur} 总时长  ·  {len(dreps)} 张  ({n_day_done}/{len(dreps)} 已标)",
                expanded=(n_day_done < len(dreps))
            ):
                img_cols = st.columns(len(dreps))
                for ci, rep in enumerate(dreps):
                    with img_cols[ci]:
                        p = Path(rep.get("image_path", ""))
                        if p.exists():
                            st.image(str(p), use_container_width=True)
                        else:
                            st.markdown("🖼 图片不存在")
                        done_mark = "✅" if rep["folder_path"] in labeled_fps else "⬜"
                        st.markdown(
                            f"<div style='font-size:12px;color:#1565C0;text-align:center'>"
                            f"{done_mark} {rep.get('collection_time','')[-5:]}<br>"
                            f"⏱ {fmt_dur(rep.get('duration',0))}</div>",
                            unsafe_allow_html=True
                        )

                st.markdown("---")
                for ri, rep in enumerate(dreps):
                    fp = rep["folder_path"]
                    is_done = fp in labeled_fps
                    with st.expander(
                        f"{'✅' if is_done else '⬜'} 图{ri+1} · {rep.get('collection_time','')} · {fmt_dur(rep.get('duration',0))}",
                        expanded=not is_done
                    ):
                        existing_dyn = get_rep_dynamic(fp)
                        form_key = f"dyn_{fp.replace('/','_')}"
                        with st.form(key=form_key):
                            dyn_tags = render_schema_section(
                                DYNAMIC_SCHEMA, existing_dyn, f"d_{fp.replace('/','_')}"
                            )
                            comments = st.text_input("备注", value="", key=f"cmt_{fp.replace('/','_')}")
                            save_btn = st.form_submit_button("💾 保存", type="primary")
                        if save_btn:
                            save_dynamic_for_rep(rep, l2, loc_static, dyn_tags, comments)
                            load_labeled_fps.clear(); load_df.clear()
                            st.success("✅ 已保存"); st.rerun()

# ─── 页面2：统计看板 ────────────────────────────────────────────────────
def page_dashboard():
    import plotly.express as px

    df = load_df()
    if df.empty:
        st.info("数据库暂无数据，请先完成标注"); return

    with st.sidebar:
        st.markdown("### 筛选")
        if st.button("🔄 刷新"):
            load_df.clear(); st.rerun()
        cities = sorted(df["city"].dropna().unique())
        sel_c  = st.multiselect("城市", cities, default=cities, key="dc")
        years  = sorted(df["year"].dropna().unique())
        sel_y  = st.multiselect("年份", years, default=years, key="dy")
        roads  = sorted(df["top_road_category"].dropna().unique())
        sel_r  = st.multiselect("道路类型", roads, default=roads, key="dr")

    df_f = df[df["city"].isin(sel_c) & df["year"].isin(sel_y) & df["top_road_category"].isin(sel_r)].copy()
    df_f["duration"] = df_f["duration"].fillna(0)

    # 概览 KPI 卡片
    n_labeled = len(df_f)
    n_cities  = df_f["city"].nunique()
    n_locs    = df_f["location_name"].nunique()
    total_dur = df_f["duration"].sum()
    hrs = total_dur / 60

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div class="kpi-card kpi-blue">
            <div class="kpi-icon">🏷️</div>
            <div class="kpi-num">{n_labeled:,}</div>
            <div class="kpi-lbl">已标注</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="kpi-card kpi-teal">
            <div class="kpi-icon">🌆</div>
            <div class="kpi-num">{n_cities}</div>
            <div class="kpi-lbl">覆盖城市</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="kpi-card kpi-amber">
            <div class="kpi-icon">📍</div>
            <div class="kpi-num">{n_locs}</div>
            <div class="kpi-lbl">地点数</div>
        </div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
        <div class="kpi-card kpi-purple">
            <div class="kpi-icon">⏱️</div>
            <div class="kpi-num">{hrs:.1f}</div>
            <div class="kpi-lbl">总时长（小时）</div>
        </div>""", unsafe_allow_html=True)

    st.divider()

    # 地点图片展示
    st.markdown("### 📸 当前筛选地点图片")
    img_df = df_f[df_f["image_path"].notna() & (df_f["image_path"] != "")].drop_duplicates("location_name").head(12)
    if not img_df.empty:
        cols = st.columns(4)
        for i, (_, row) in enumerate(img_df.iterrows()):
            with cols[i % 4]:
                p = Path(row["image_path"])
                if p.exists():
                    st.image(str(p), use_container_width=True)
                st.markdown(
                    f"<div class='img-card-meta' style='text-align:center'>"
                    f"<b>{row['city']}</b> · {row['location_name']}<br>"
                    f"<span style='color:#5a7fae'>⏱ {fmt_dur(row['duration'])}</span></div>",
                    unsafe_allow_html=True
                )
    else:
        st.info("当前筛选结果无图片")

    st.divider()

    # 地域分布
    st.markdown("### 📍 地域分布")
    c1, c2 = st.columns(2)
    with c1:
        cd = df_f.groupby("city").agg(数量=("id","count"), 时长=("duration","sum")).reset_index()
        cd["时长(h)"] = cd["时长"] / 60
        if not cd.empty:
            fig = px.bar(cd, x="city", y="数量", text="数量", color="city",
                         color_discrete_sequence=BLUE, title="各城市标注数量",
                         labels={"city":"城市"})
            fig.update_traces(textposition="outside", textfont=dict(size=11, color="#0d2d5e"),
                              marker_line_width=0, opacity=0.88)
            fig.update_layout(**_cl(), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        ld = df_f.groupby("location_name").agg(时长=("duration","sum")).reset_index()
        ld["时长(h)"] = ld["时长"] / 60
        ld = ld.sort_values("时长(h)", ascending=False).head(15)
        if not ld.empty:
            fig = px.bar(ld, x="location_name", y="时长(h)", color="location_name",
                         color_discrete_sequence=BLUE, title="各地点视频总时长 (Top 15)",
                         labels={"location_name":"地点","时长(h)":"时长(h)"})
            fig.update_traces(marker_line_width=0, opacity=0.88)
            fig.update_layout(**_cl(), showlegend=False, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    # 采集时段
    st.markdown("### 🕐 采集时段分布")
    df_f["_h"] = df_f["collection_time"].apply(parse_hour)
    df_f["period"] = df_f["_h"].apply(time_period)
    order = ["早高峰 (07-09)","日常规","晚高峰 (17-19)","夜间 (19+)"]
    pd_df = df_f.groupby("period").size().reset_index(name="数量")
    pd_df["_o"] = pd_df["period"].apply(lambda x: order.index(x) if x in order else 99)
    pd_df = pd_df.sort_values("_o").drop(columns=["_o"])
    if not pd_df.empty:
        fig = px.bar(pd_df, x="period", y="数量", text="数量", color="period",
                     color_discrete_sequence=BLUE, title="采集时段分布",
                     labels={"period":"时段"})
        fig.update_traces(textposition="outside", textfont=dict(size=11, color="#0d2d5e"),
                          marker_line_width=0, opacity=0.88)
        fig.update_layout(**_cl(), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # 道路类型旭日图
    st.markdown("### 🛣️ 道路类型分布")
    top_df = df_f[df_f["top_road_category"].notna() & (df_f["top_road_category"] != "")]
    if not top_df.empty:
        rows = []
        for _, row in top_df.iterrows():
            vals = extract_tag(row["_tags"], "一、道路静态环境","1.6 道路交叉","交叉类型")
            for ct in (vals or ["未标注"]):
                rows.append({"主类":row["top_road_category"],
                             "子类":row.get("top_road_subcategory",""),
                             "交叉类型":ct})
        if rows:
            sb = pd.DataFrame(rows).groupby(["主类","子类","交叉类型"]).size().reset_index(name="数量")
            fig = px.sunburst(sb, path=["主类","子类","交叉类型"], values="数量",
                              title="道路类型层级分布", color_discrete_sequence=BLUE)
            fig.update_traces(textfont=dict(size=11))
            fig.update_layout(**_cl(), margin=dict(t=52, l=0, r=0, b=0))
            st.plotly_chart(fig, use_container_width=True)

    # 标签分布
    st.markdown("### 🏷️ 标签分布")
    tag_opts = [f"{sub} · {attr}" for _, sub, attr in TAG_PATHS]
    sel_tag = st.selectbox("标签维度", tag_opts, key="dash_tag")
    sec, sub, attr = TAG_PATHS[tag_opts.index(sel_tag)]
    items = []
    for d in df_f["_tags"]:
        v = extract_tag(d, sec, sub, attr)
        if v: items.extend(v)
    if items:
        cnt = pd.Series(items).value_counts().reset_index()
        cnt.columns = ["标签值","数量"]
        ct = st.radio("图表", ["柱状图","饼图"], horizontal=True, key="dchart")
        if ct == "饼图":
            fig = px.pie(cnt, values="数量", names="标签值",
                         title=f"{sub} · {attr}", color_discrete_sequence=BLUE, hole=0.38)
            fig.update_traces(texttemplate="%{label}<br>%{percent:.1%}",
                              textposition="outside", textfont=dict(size=11),
                              pull=[0.03] * len(cnt))
        else:
            fig = px.bar(cnt, x="标签值", y="数量", text="数量", color="标签值",
                         color_discrete_sequence=BLUE, title=f"{sub} · {attr}")
            fig.update_traces(textposition="outside", textfont=dict(size=11, color="#0d2d5e"),
                              marker_line_width=0, opacity=0.88)
            fig.update_layout(**_cl(), showlegend=False, xaxis_tickangle=-30)
        fig.update_layout(**_cl())
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("该维度暂无数据")

    # 交叉分析
    st.markdown("### 📈 交叉分析")
    DIMS = [("city","城市"),("top_road_category","道路主类"),
            ("top_road_subcategory","道路子类"),("period","时段"),("year","年份")]
    lbls = [v for _,v in DIMS]; keys = [k for k,_ in DIMS]
    dc1, dc2 = st.columns(2)
    with dc1: s1 = st.selectbox("维度1", lbls, key="cx1")
    with dc2: s2 = st.selectbox("维度2", lbls, index=1, key="cx2")
    k1, k2 = keys[lbls.index(s1)], keys[lbls.index(s2)]
    if k1 != k2 and all(k in df_f.columns for k in (k1, k2)):
        pivot = df_f.groupby([k1, k2]).size().unstack(fill_value=0)
        st.dataframe(pivot.style.background_gradient(cmap="Blues"), use_container_width=True)

    with st.expander("📋 原始数据"):
        cols = ["folder_path","city","location_name","top_road_category","top_road_subcategory","collection_time","duration"]
        st.dataframe(df_f[[c for c in cols if c in df_f.columns]].head(200), use_container_width=True)


# ─── 页面3：新增地点 ────────────────────────────────────────────────────
def page_add():
    st.markdown("### 新增地点标注")
    st.markdown(
        "<div class='tip'>用于没有视频文件但需要录入标注数据的地点，"
        "或补录已采集地点的额外信息。</div>", unsafe_allow_html=True
    )

    with st.form("add_loc_form"):
        st.markdown("#### 基本信息")
        r1c1, r1c2, r1c3 = st.columns(3)
        with r1c1:
            city_input = st.selectbox("城市", list(CITY_MAP.values()) + ["其他"], key="add_city")
        with r1c2:
            year_input = st.text_input("年份", value=str(datetime.now().year), key="add_year")
        with r1c3:
            loc_name = st.text_input("地点名称 (英文/编号)", key="add_loc")

        r2c1, r2c2 = st.columns(2)
        with r2c1:
            coll_date = st.date_input("采集日期", key="add_date")
        with r2c2:
            coll_hour = st.slider("采集小时", 0, 23, 9, key="add_hour")

        duration_min = st.number_input("视频总时长 (分钟)", min_value=0.0, step=1.0, key="add_dur")
        comments = st.text_area("备注", height=60, key="add_cmt")

        st.markdown("#### 图片")
        img_mode = st.radio("图片来源", ["上传图片", "输入路径"], horizontal=True, key="add_imgmode")
        uploaded_file = None
        img_path_input = ""
        if img_mode == "上传图片":
            uploaded_file = st.file_uploader("上传代表图 (JPG/PNG)", type=["jpg","jpeg","png"], key="add_file")
        else:
            img_path_input = st.text_input("图片绝对路径", key="add_imgpath")

        st.markdown("#### 道路类型")
        cats = list(TOP_LEVEL_CONFIG.keys())
        ac1, ac2 = st.columns(2)
        with ac1:
            top_cat = st.selectbox("道路主类", cats, key="add_cat")
        with ac2:
            subs = TOP_LEVEL_CONFIG[top_cat]
            top_sub = st.selectbox("道路子类", subs, key="add_sub")

        st.markdown("#### 静态标签")
        static_tags = render_schema_section(STATIC_SCHEMA, {}, "add_st")
        st.markdown("#### 动态标签")
        dynamic_tags = render_schema_section(DYNAMIC_SCHEMA, {}, "add_dy")

        submitted = st.form_submit_button("💾 保存新地点", type="primary", use_container_width=True)

    if submitted:
        if not loc_name.strip():
            st.error("地点名称不能为空"); return

        # 处理图片
        final_img_path = ""
        if img_mode == "上传图片" and uploaded_file:
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            save_path = UPLOAD_DIR / f"{loc_name}_{uploaded_file.name}"
            save_path.write_bytes(uploaded_file.read())
            final_img_path = str(save_path)
        elif img_path_input:
            final_img_path = img_path_input

        folder_path    = f"MANUAL/{city_input}/{year_input}/{loc_name}"
        collection_time = f"{coll_date.year}年{coll_date.month}月{coll_date.day}日 {coll_hour:02d}:00"
        all_tags = {**static_tags, **dynamic_tags}

        save_manual_location(
            folder_path=folder_path,
            video_name="MANUAL",
            location_name=loc_name,
            collection_time=collection_time,
            top_cat=top_cat,
            top_sub=top_sub,
            all_tags=all_tags,
            duration=float(duration_min),
            image_path=final_img_path,
            comments=comments,
        )
        load_df.clear()
        st.success(f"✅ 新地点「{loc_name}」已保存！folder_path = {folder_path}")


def _cl() -> dict:
    return dict(
        plot_bgcolor="rgba(255,255,255,0.00)",
        paper_bgcolor="rgba(255,255,255,0.55)",
        title_font=dict(size=14, color="#0d2d5e", family="'Helvetica Neue', Arial, sans-serif"),
        title_x=0.02,
        font=dict(family="'Helvetica Neue', 'PingFang SC', Arial, sans-serif",
                  size=12, color="#2d3748"),
        xaxis=dict(showgrid=False, showline=False,
                   tickfont=dict(size=11, color="#4a6fa5"),
                   tickcolor="rgba(0,0,0,0)"),
        yaxis=dict(gridcolor="rgba(160,174,192,0.22)", gridwidth=1,
                   showline=False, zeroline=False,
                   tickfont=dict(size=11, color="#4a6fa5")),
        legend=dict(bgcolor="rgba(255,255,255,0.65)",
                    bordercolor="rgba(99,179,237,0.25)", borderwidth=1,
                    font=dict(size=11, color="#2d3748")),
        hoverlabel=dict(bgcolor="rgba(255,255,255,0.92)", font_size=12,
                        font_color="#0d2d5e", bordercolor="rgba(99,179,237,0.40)"),
        coloraxis_showscale=False,
    )

# ─── CSS ───────────────────────────────────────────────────────────────
CSS = """
<style>
/* ════════════════════════════════════════
   驭研科技 · 专业深蓝主题 (ODD 标注平台)
   Primary  : #0d2d5e   (深海蓝)
   Accent   : #1a56a8   (品牌蓝)
   Highlight: #3b7dd8   (亮蓝)
   Surface  : #eef2f9   (浅灰蓝背景)
════════════════════════════════════════ */

/* ── 应用主体 ── */
.stApp { background: #eef2f9 !important; }

/* ── 主内容区 ── */
.block-container {
    background: rgba(255,255,255,0.95) !important;
    border-radius: 16px !important;
    padding: 1.8rem 2.4rem !important;
    box-shadow: 0 2px 20px rgba(13,45,94,0.08) !important;
    border: 1px solid rgba(26,86,168,0.12) !important;
}

/* ══════════════════════════════════════
   品牌 Header · 科技帝国旗舰版
══════════════════════════════════════ */
.brand-header {
    background: linear-gradient(120deg, #010c1f 0%, #021630 20%, #0b2a58 45%, #0e3570 60%, #0b2a58 80%, #010c1f 100%);
    border-radius: 18px;
    padding: 1.6rem 2.2rem 1.5rem;
    margin-bottom: 1.6rem;
    display: flex;
    align-items: center;
    gap: 1.8rem;
    position: relative;
    overflow: hidden;
    border-top: 1px solid rgba(100,170,255,0.25);
    border-left: 1px solid rgba(80,140,255,0.18);
    border-right: 1px solid rgba(40,80,180,0.15);
    border-bottom: 2px solid rgba(59,125,216,0.5);
    box-shadow:
        0 8px 48px rgba(2,14,40,0.7),
        0 2px 0 rgba(59,125,216,0.6),
        inset 0 1px 0 rgba(120,180,255,0.15),
        inset 0 -1px 0 rgba(20,60,160,0.3);
}
/* 流光扫描动画 */
.brand-header::before {
    content: '';
    position: absolute; top: 0; left: -80%; width: 55%; height: 100%;
    background: linear-gradient(90deg,
        transparent 0%,
        rgba(100,170,255,0.03) 30%,
        rgba(120,190,255,0.07) 50%,
        rgba(100,170,255,0.03) 70%,
        transparent 100%
    );
    animation: header-sweep 6s ease-in-out infinite;
    pointer-events: none; z-index: 1;
}
@keyframes header-sweep {
    0%   { left: -80%; }
    100% { left: 130%; }
}
/* 右侧双层光晕 */
.brand-header::after {
    content: '';
    position: absolute; top: -60%; right: -10%;
    width: 55%; height: 220%;
    background:
        radial-gradient(ellipse 60% 60% at 70% 50%, rgba(59,125,216,0.22) 0%, transparent 65%),
        radial-gradient(ellipse 30% 40% at 85% 40%, rgba(100,180,255,0.12) 0%, transparent 60%);
    pointer-events: none; z-index: 0;
}
/* 左上角 tech bracket */
.brand-header .tech-corner-tl {
    position: absolute; top: 10px; left: 12px;
    width: 18px; height: 18px;
    border-top: 2px solid rgba(80,160,255,0.6);
    border-left: 2px solid rgba(80,160,255,0.6);
    border-radius: 3px 0 0 0;
    z-index: 3;
}
/* 右下角 tech bracket */
.brand-header .tech-corner-br {
    position: absolute; bottom: 10px; right: 12px;
    width: 18px; height: 18px;
    border-bottom: 2px solid rgba(80,160,255,0.6);
    border-right: 2px solid rgba(80,160,255,0.6);
    border-radius: 0 0 3px 0;
    z-index: 3;
}
.brand-icon {
    font-size: 2.2rem; position: relative; z-index: 2;
    filter: drop-shadow(0 0 14px rgba(80,160,255,0.85));
}
.brand-text { flex: 1; position: relative; z-index: 2; }
.brand-title {
    font-size: 1.85rem; font-weight: 900; color: #ffffff;
    letter-spacing: 4px; line-height: 1.2; margin-top: 4px;
    background: linear-gradient(90deg, #ffffff 0%, #cce4ff 40%, #ffffff 70%, #a8d0ff 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
    filter: drop-shadow(0 0 12px rgba(80,150,255,0.8)) drop-shadow(0 0 30px rgba(59,125,216,0.5));
    animation: title-glow 4s ease-in-out infinite;
}
@keyframes title-glow {
    0%, 100% { filter: drop-shadow(0 0 10px rgba(80,150,255,0.7)) drop-shadow(0 0 28px rgba(59,125,216,0.4)); }
    50%       { filter: drop-shadow(0 0 18px rgba(120,190,255,1.0)) drop-shadow(0 0 45px rgba(80,160,255,0.65)); }
}
.brand-sub {
    font-size: 0.72rem; color: rgba(140,195,255,0.7);
    letter-spacing: 3px; margin-top: 7px;
    text-transform: uppercase; font-weight: 600;
}
.brand-badge {
    font-size: 0.70rem; font-weight: 700; color: rgba(180,220,255,0.95);
    background: linear-gradient(135deg, rgba(20,60,140,0.5), rgba(10,40,100,0.4));
    border: 1px solid rgba(80,150,255,0.5);
    border-radius: 22px; padding: 6px 18px; white-space: nowrap;
    position: relative; z-index: 2;
    box-shadow: 0 0 16px rgba(59,125,216,0.4), inset 0 1px 0 rgba(120,180,255,0.2);
    animation: pulse-badge 3s ease-in-out infinite;
    letter-spacing: 1px;
}
@keyframes pulse-badge {
    0%, 100% { box-shadow: 0 0 16px rgba(59,125,216,0.4), inset 0 1px 0 rgba(120,180,255,0.2); }
    50%       { box-shadow: 0 0 28px rgba(80,160,255,0.7), inset 0 1px 0 rgba(150,210,255,0.35); }
}

/* ── 侧边栏（深蓝渐变 + 科技质感）── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #010c1f 0%, #0b2a58 30%, #0e3570 60%, #0b2a58 100%) !important;
    border-right: 1px solid rgba(80,150,255,0.2);
}
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span { color: rgba(200,225,255,0.85) !important; }
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 { color: white !important; }
[data-testid="stSidebar"] [data-baseweb="tag"] {
    background: rgba(59,125,216,0.4) !important;
    border: 1px solid rgba(80,160,255,0.4) !important;
    color: #cce4ff !important;
}
[data-testid="stSidebar"] hr { border-color: rgba(80,150,255,0.25) !important; }
[data-testid="stSidebar"] button {
    background: rgba(255,255,255,0.06) !important;
    color: rgba(200,225,255,0.95) !important;
    border: 1px solid rgba(80,150,255,0.25) !important;
    border-radius: 8px !important;
    text-align: left !important;
    font-size: 0.82rem !important;
    transition: all 0.18s ease !important;
}
[data-testid="stSidebar"] button:hover {
    background: rgba(59,125,216,0.22) !important;
    border-color: rgba(80,160,255,0.5) !important;
    transform: translateX(3px) !important;
    box-shadow: 0 2px 12px rgba(59,125,216,0.3) !important;
}

/* ── KPI 渐变卡片 ── */
.kpi-card {
    border-radius: 14px; padding: 20px 14px 16px;
    text-align: center; color: white; min-height: 128px;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    margin-bottom: 6px; position: relative; overflow: hidden;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.kpi-card:hover { transform: translateY(-3px); }
.kpi-card::after {
    content:''; position:absolute; top:-35%; right:-15%;
    width:55%; height:130%; background:rgba(255,255,255,0.09);
    border-radius:50%; pointer-events:none;
}
.kpi-icon { font-size: 1.6rem; margin-bottom: 2px; }
.kpi-num  { font-size: 2.5rem; font-weight: 800; line-height: 1.05;
            letter-spacing: -1.5px; margin: 2px 0; }
.kpi-lbl  { font-size: 0.70rem; font-weight: 600; text-transform: uppercase;
            letter-spacing: 0.9px; opacity: 0.85; margin-top: 3px; }
.kpi-blue   { background: linear-gradient(135deg,#1a56a8 0%,#0d2d5e 100%);
              box-shadow: 0 6px 20px rgba(13,45,94,.40); }
.kpi-teal   { background: linear-gradient(135deg,#2563eb 0%,#1e40af 100%);
              box-shadow: 0 6px 20px rgba(37,99,235,.38); }
.kpi-amber  { background: linear-gradient(135deg,#0891b2 0%,#0369a1 100%);
              box-shadow: 0 6px 20px rgba(8,145,178,.38); }
.kpi-purple { background: linear-gradient(135deg,#4338ca 0%,#312e81 100%);
              box-shadow: 0 6px 20px rgba(67,56,202,.38); }

/* ── Metric 卡片（覆盖默认） ── */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(238,242,249,0.9), rgba(187,222,251,0.6));
    border-radius: 12px; padding: 16px 20px;
    border-left: 4px solid #1a56a8;
    box-shadow: 0 2px 12px rgba(13,45,94,0.10);
    border: 1px solid rgba(26,86,168,0.14);
}
[data-testid="stMetricLabel"] p { color: #1a56a8 !important; font-weight: 600; }
[data-testid="stMetricValue"]   { color: #0d2d5e !important; }

/* ── 标题 ── */
h2, h3 { color: #0d2d5e !important; }
.stSubheader > div {
    border-bottom: 2px solid rgba(26,86,168,0.20) !important;
    padding-bottom: 0.25rem !important; color: #0d2d5e !important;
}
h2 { padding-bottom: 6px; border-bottom: 2px solid rgba(26,86,168,0.20); margin-bottom: 16px; }
h3 { margin-top: 20px; }
hr { border-color: rgba(26,86,168,0.15) !important; }

/* ── 地点 Header（高级渐变版） ── */
.loc-header {
    background: linear-gradient(120deg, #010c1f 0%, #0b2a58 40%, #0e3570 60%, #0b2a58 100%);
    color: white; border-radius: 14px; padding: 18px 26px;
    margin-bottom: 20px; display: flex; align-items: center;
    gap: 18px; flex-wrap: wrap;
    border: 1px solid rgba(80,150,255,0.2);
    box-shadow: 0 6px 30px rgba(2,14,40,0.35), 0 1px 0 rgba(59,125,216,0.4);
    position: relative; overflow: hidden;
}
.loc-header::before {
    content: '';
    position: absolute; top: 0; right: 0; width: 40%; height: 100%;
    background: radial-gradient(ellipse at 80% 50%, rgba(59,125,216,0.18), transparent 70%);
    pointer-events: none;
}
.loc-city {
    font-size: 20px; font-weight: 800; letter-spacing: 1px;
    background: linear-gradient(90deg, #fff, #cce4ff);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
}
.loc-name { font-size: 15px; opacity: .85; font-weight: 500; }
.loc-stat { font-size: 13px; opacity: .75; margin-left: auto; letter-spacing: 0.3px; }

/* ── 分类标签栏 ── */
.sec-bar {
    background: linear-gradient(90deg, #0d2d5e, #1a56a8, #3b7dd8);
    color: white; padding: 6px 16px; border-radius: 8px;
    font-weight: 700; font-size: 13px; margin: 12px 0 8px 0;
    letter-spacing: 0.5px;
    box-shadow: 0 2px 8px rgba(13,45,94,0.25);
}

/* ── 提示条 ── */
.tip {
    background: linear-gradient(135deg, rgba(238,242,249,0.95), rgba(187,222,251,0.4));
    border-left: 4px solid #1a56a8;
    border-radius: 8px; padding: 10px 16px; font-size: 13px;
    color: #2d4a6f; margin-bottom: 14px;
    border: 1px solid rgba(26,86,168,0.12);
}

/* ── 表单容器 ── */
[data-testid="stForm"] {
    border: 1px solid rgba(26,86,168,0.18); border-radius: 14px;
    padding: 18px; background: rgba(250,252,255,0.95);
    box-shadow: 0 2px 12px rgba(13,45,94,0.05);
}

/* ── 主要按钮 ── */
.stButton>button[kind="primary"] {
    background: linear-gradient(90deg, #0d2d5e, #1a56a8) !important;
    border: none !important; border-radius: 8px !important; font-weight: 700 !important;
    letter-spacing: 0.5px !important;
    box-shadow: 0 3px 12px rgba(13,45,94,0.3) !important;
    transition: all 0.18s ease !important;
}
.stButton>button[kind="primary"]:hover {
    background: linear-gradient(90deg, #1a56a8, #3b7dd8) !important;
    box-shadow: 0 6px 20px rgba(26,86,168,0.4) !important;
    transform: translateY(-1px) !important;
}

/* ── 通用按钮 ── */
[data-testid="stMain"] .stButton > button {
    font-size: 0.85rem !important; font-weight: 500 !important;
    min-height: 34px !important; padding: 0.3rem 0.8rem !important;
    color: #1a56a8 !important;
    background: rgba(26,86,168,0.07) !important;
    border: 1px solid rgba(26,86,168,0.28) !important;
    border-radius: 8px !important; transition: all 0.17s ease !important;
}
[data-testid="stMain"] .stButton > button:hover {
    background: rgba(26,86,168,0.14) !important;
    border-color: rgba(26,86,168,0.50) !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 3px 10px rgba(26,86,168,0.13) !important;
}

/* ── 进度条 ── */
[data-testid="stProgressBar"]>div { background: linear-gradient(90deg, #1a56a8, #3b7dd8) !important; }

/* ── Multiselect 标签 ── */
[data-testid="stMultiSelect"] [data-baseweb="tag"] {
    background: rgba(26,86,168,0.10) !important;
    border: 1px solid rgba(26,86,168,0.28) !important;
    border-radius: 6px !important; font-size: 0.75rem !important;
    color: #0d2d5e !important;
}
[data-testid="stMultiSelect"] label {
    font-size: 0.79rem !important; font-weight: 600 !important;
    color: #3d5f8f !important;
}

/* ── Border 容器 ── */
[data-testid="stVerticalBlockBorderWrapper"] {
    background: #ffffff !important;
    border: 1px solid rgba(26,86,168,0.16) !important;
    border-radius: 13px !important;
}

/* ── Expander ── */
details summary {
    background: rgba(230,240,255,0.75) !important;
    border-radius: 9px !important; padding: 0.5rem 1rem !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border-radius: 9px !important; overflow: hidden !important;
    border: 1px solid rgba(26,86,168,0.14) !important;
}

/* ── Caption ── */
.stCaption { color: #3d5f8f !important; }

/* ── Tabs 样式优化 ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: rgba(26,86,168,0.04);
    border-radius: 10px;
    padding: 3px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    font-weight: 600;
    color: #3d5f8f;
}
.stTabs [aria-selected="true"] {
    background: rgba(26,86,168,0.12) !important;
    color: #0d2d5e !important;
}

/* ── ODD 标签详情面板 ── */
.odd-section { margin-bottom: 14px; }
.odd-sec-hd {
    font-size: 0.87rem; font-weight: 700; color: white;
    padding: 4px 15px; border-radius: 20px; margin-bottom: 9px;
    display: inline-block; letter-spacing: 0.3px;
}
.sec-road    { background: linear-gradient(90deg,#1a56a8,#0d2d5e); }
.sec-infra   { background: linear-gradient(90deg,#0e7490,#155e75); }
.sec-dynamic { background: linear-gradient(90deg,#b45309,#92400e); }
.sec-atmo    { background: linear-gradient(90deg,#4338ca,#312e81); }
.odd-row {
    margin: 5px 0 5px 12px;
    display: flex; flex-wrap: wrap; align-items: center; gap: 6px;
}
.odd-attr {
    font-size: 0.82rem; color: #3d5f8f; font-weight: 600;
    min-width: 158px; flex-shrink: 0;
}
.odd-tag {
    display: inline-block; padding: 3px 12px; border-radius: 13px;
    font-size: 0.83rem; font-weight: 600; white-space: nowrap;
}
.tag-road    { background:rgba(26,86,168,0.10);  color:#0d2d5e; border:1px solid rgba(26,86,168,0.26); }
.tag-infra   { background:rgba(14,116,144,0.11); color:#155e75; border:1px solid rgba(14,116,144,0.26); }
.tag-dynamic { background:rgba(180,83,9,0.10);   color:#92400e; border:1px solid rgba(180,83,9,0.25); }
.tag-atmo    { background:rgba(67,56,202,0.10);  color:#312e81; border:1px solid rgba(67,56,202,0.25); }

/* ── 筛选卡片 ── */
.filter-card {
    background: #ffffff;
    border: 1px solid rgba(26,86,168,0.16);
    border-radius: 14px; padding: 1rem 1.4rem 0.8rem;
    margin-bottom: 1rem; box-shadow: 0 2px 10px rgba(13,45,94,0.06);
}
.filter-badge {
    display: inline-block; background: rgba(26,86,168,0.10); color: #0d2d5e;
    border: 1px solid rgba(26,86,168,0.25); border-radius: 20px;
    padding: 2px 10px; font-size: 0.74rem; font-weight: 600;
    margin: 2px 3px; vertical-align: middle;
}

/* ── 地点关键标签摘要行 ── */
.loc-key-summary {
    display: flex; flex-wrap: wrap; gap: 8px;
    background: linear-gradient(135deg,rgba(13,45,94,0.04),rgba(26,86,168,0.06));
    border: 1px solid rgba(26,86,168,0.14);
    border-radius: 10px; padding: 10px 14px; margin: 8px 0 14px;
}
.loc-key-item {
    display: flex; align-items: center; gap: 5px;
    font-size: 0.82rem; color: #1a3a6e;
}
.loc-key-label {
    font-size: 0.70rem; font-weight: 700; color: #5a7fae;
    text-transform: uppercase; letter-spacing: 0.4px;
}
.loc-key-val {
    background: rgba(26,86,168,0.09); color: #0d2d5e;
    border: 1px solid rgba(26,86,168,0.22); border-radius: 12px;
    padding: 2px 9px; font-size: 0.80rem; font-weight: 600;
}

/* ── 侧边栏导航项图标按钮 ── */
.nav-item {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 16px; border-radius: 10px;
    color: rgba(200,225,255,0.95); font-weight: 600;
    font-size: 0.88rem; cursor: pointer;
    transition: all 0.18s ease;
    border: 1px solid transparent;
}
.nav-item:hover {
    background: rgba(59,125,216,0.18);
    border-color: rgba(80,150,255,0.3);
}
.nav-item.active {
    background: rgba(59,125,216,0.25);
    border-color: rgba(80,160,255,0.5);
    color: white;
}

/* ── 图片网格卡片 ── */
.img-card {
    border-radius: 10px; overflow: hidden;
    border: 1px solid rgba(26,86,168,0.18);
    background: #fff;
    box-shadow: 0 2px 10px rgba(13,45,94,0.08);
    transition: transform 0.18s, box-shadow 0.18s;
    margin-bottom: 8px;
}
.img-card:hover { transform: translateY(-3px); box-shadow: 0 6px 20px rgba(13,45,94,0.16); }
.img-card-meta {
    padding: 6px 10px 8px;
    font-size: 0.72rem; color: #2a4a7f; line-height: 1.55;
}
.img-card-meta b { color: #0d2d5e; }
</style>
"""

# ─── 主入口 ────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="驭研科技 · ODD 标注平台", page_icon="🛰️",
                       layout="wide", initial_sidebar_state="expanded")
    st.markdown(CSS, unsafe_allow_html=True)
    init_db()

    # ── Brand Header ──
    st.markdown("""
<div class="brand-header">
  <div class="tech-corner-tl"></div>
  <div class="tech-corner-br"></div>
  <div class="brand-icon">🛰️</div>
  <div class="brand-text">
    <div class="brand-title">ODD 标注平台</div>
    <div class="brand-sub">DRIVEResearch · Operational Design Domain Labeling Platform</div>
  </div>
  <div class="brand-badge">🗺️ Aerial · ODD Labeling</div>
</div>
""", unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("""
<div style="text-align:center; padding: 1rem 0 0.5rem;">
    <div style="font-size:2rem; margin-bottom:4px;">🛰️</div>
    <div style="font-size:1.1rem; font-weight:800; color:white; letter-spacing:2px;
                background:linear-gradient(90deg,#fff,#a8d0ff);
                -webkit-background-clip:text; -webkit-text-fill-color:transparent;">
        ODD 标注平台
    </div>
    <div style="font-size:0.65rem; color:rgba(140,195,255,0.6); letter-spacing:1.5px; margin-top:3px;">
        DRIVERESEARCH
    </div>
</div>
""", unsafe_allow_html=True)
        st.markdown("---")
        page = st.radio("", ["🏷️ 地点标注","📊 统计看板","➕ 新增地点"],
                        label_visibility="collapsed", key="nav")
        st.markdown("---")

    if page == "🏷️ 地点标注":
        st.markdown("## 🏷️ 地点标注")
        page_label()
    elif page == "📊 统计看板":
        st.markdown("## 📊 统计看板")
        page_dashboard()
    else:
        st.markdown("## ➕ 新增地点")
        page_add()


if __name__ == "__main__":
    main()
