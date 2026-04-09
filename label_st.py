import streamlit as st
import pandas as pd
import sqlite3
import json
import os
import copy
from pathlib import Path
from datetime import datetime, time

# 支持的视频文件扩展名
VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mkv', '.mov', '.flv', '.wmv', '.m4v', '.webm'}

import re

def parse_collection_time_from_filename(video_name: str) -> str:
    """从视频文件名解析采集日期时间，格式如 DJI_20250711155546_0001_V → 2025年7月11日 15:55"""
    m = re.search(r'(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})', video_name)
    if m:
        y, mo, d, h, mi = m.groups()
        return f"{y}年{int(mo)}月{int(d)}日 {int(h):02d}:{int(mi):02d}"
    return ""

def parse_collection_hour_minute(video_name: str) -> tuple:
    """从视频文件名解析采集时间的小时和分钟，返回 (hour, minute) 或 (None, None)"""
    m = re.search(r'(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})', video_name)
    if m:
        h, mi = int(m.group(4)), int(m.group(5))
        return (h, mi)
    return (None, None)

def infer_lighting_from_time(hour: int, minute: int, sunset_h: int, sunset_m: int, dark_h: int, dark_m: int) -> dict:
    """根据采集时间推断 4.3 光照标签。日落前=正常，日落至天黑=昏暗，天黑后=黑暗"""
    total_mins = hour * 60 + minute
    sunset_mins = sunset_h * 60 + sunset_m
    dark_mins = dark_h * 60 + dark_m
    if total_mins < sunset_mins:
        return {"来源": "自然光", "强度": "正常"}
    elif total_mins < dark_mins:
        return {"来源": "自然光", "强度": "弱光/昏暗"}
    else:
        return {"来源": "自然光", "强度": "黑暗"}

# ==========================================
# 1. 标签体系配置 (基于思维导图复刻)
# ==========================================

# 顶层标签：地点与场景 (对应图中蓝色根节点1)
TOP_LEVEL_CONFIG = {
    "区域": ["封闭园区", "交通管制区域", "开放道路"],
    "城市道路": ["快速路", "主干路", "次干路", "支路", "街巷"],
    "公路": ["高速公路", "一级公路", "二级公路", "三级公路", "四级公路"],
    "乡村道路": ["村道", "其他乡村内部道路"],
    "其他道路": ["厂矿", "林区", "港口", "专用道路"],
    "停车区域": ["室内停车场", "室外停车场", "路侧停车位"],
    "自动驾驶场景": ["封闭场景", "半封闭场景", "开放场景"]
}

# 次级标签：静态环境与动态目标 (对应图中蓝色根节点2)
# 结构：大类 -> 子类 -> 属性维度 -> 选项列表
# 多选字段配置：子类名 -> [属性名列表]
SECONDARY_MULTI_SELECT = {
    "1.3 道路几何": ["曲率"],
    "1.4 包含车道特征": ["车道类型", "车道宽度"],
    "2.1 交通控制": ["地面标签"],
    "2.2 路侧与周边环境": ["设施"],
    "3.1 机动车": ["类型"],
    "3.2 VRU": ["类型"],
    "3.4 障碍物": ["类型"],
}

SECONDARY_LEVEL_CONFIG = {
    "一、道路静态环境": {
        "1.2 道路表面": {
            "表面类型": ["沥青", "混凝土", "土路", "碎石", "冰雪路面", "金属板"],
            "表面状态": ["干燥", "潮湿", "积水", "积雪", "结冰", "泥泞"]
        },
        "1.3 道路几何": {
            "坡度": ["平路", "上坡", "下坡", "起伏路"],
            "曲率": ["直线", "弯道 (曲率<0.01)", "弯道 (0.01<曲率<0.05)", "弯道 (曲率>0.05)"],
            "横坡": ["正常排水坡度", "反超高", "无横坡"]
        },
        "1.4 包含车道特征": {
            "最宽车道数量": ["单车道", "双车道", "三车道", "四车道及以上"],
            "车道类型": ["普通车道", "公交专用道", "HOV车道", "潮汐车道", "应急车道", "非机动车道", "人行道",'汇入匝道','汇出匝道'],
            "车道宽度": ["标准", "狭窄", "超宽"]
        },
        "1.5 道路边缘": {
            "边缘类型": ["路缘石", "护栏 (金属)", "护栏 (混凝土)", "草地/泥土", "无物理隔离"]
        },
        "1.6 道路交叉": {
            "交叉类型": ["路段 (无交叉)", "平面交叉 (十字)", "平面交叉 (丁字)",'平面交叉 (畸形)', "大型环岛 (出入口数 > 4)", "小型环岛", "立体交叉"]
        }
    },
    "二、交通设施": {
        "2.1 交通控制": {
            "信号灯": ["有", "无"],
            "标志牌": ["限速", "禁止", "指示", "警告", "施工","无"],
            "地面标签": ["实线", "虚线", "双黄线", "导流线", "斑马线", "标线磨损"]
        },
        "2.2 路侧与周边环境": {
            "设施": ["无","路灯", "电线杆", "隔音墙", "路边树木", "路边停车位","地面停车场出入口",'隧道出入口', "居民楼", "商场", "学校", "医院", "公园", "绿化带"]
        },
        "2.3 特殊设施": {
            "类型": ["收费站", "检查站", "施工区域围挡", "减速带","无"]
        }
    },
    "三、动态目标 (路面状况)": {
        "3.1 机动车": {
            "类型": ["轿车", "客车/巴士", "卡车/货车", "特种车辆 (警)","特种车辆(消)","特种车辆(救)","工程车辆"]
        },
        "3.2 VRU": {
            "类型": ["自行车", "电动车", "三轮车", "行人","无"]
        },
        "3.3 动物": {
            "类型": ["有", "无"]
        },
        "3.4 障碍物": {
            "类型": ["落石", "遗洒物", "倒伏树木", "锥桶","无"]
        },
        "3.5 事故车辆": {
            "类型": ["有", "无"]
        }
    },
    "四、大气环境": {
        "4.1 天气": {
            "类型": ["晴", "多云", "阴", "雨 (小/中/大)", "雪", "雾", "冰雹"]
        },
        "4.2 颗粒物": {
            "类型": ["无", "雾霾", "沙尘", "烟尘"]
        },
        "4.3 光照": {
            "来源": ["自然光", "人工照明", "混合光"],
            "强度": ["正常", "强光/逆光", "弱光/昏暗", "黑暗"]
        },
        "4.4 气温": {
            "估算": ["极寒 (< -20℃)", "寒冷 (-20℃ ~ -10℃)", "舒适 (-10℃ ~ 10℃)", "炎热 (10℃ ~ 20℃)", "极热 (> 20℃)"] # 
        }
    }
}

# ==========================================
# 1.5 文件夹分析工具
# ==========================================

def get_folder_info(folder_path: str) -> dict:
    """分析文件夹：计算大小、估算时长(1G/分钟)、列出视频文件"""
    folder_path = folder_path.strip().rstrip(os.sep)
    result = {
        "valid": False,
        "path": folder_path,
        "total_bytes": 0,
        "total_size_gb": 0.0,
        "estimated_minutes": 0.0,
        "estimated_duration_str": "",
        "video_files": [],
        "file_tree": [],
        "location_name": "",
        "error": ""
    }
    if not folder_path:
        result["error"] = "请输入文件夹路径"
        return result
    if not os.path.isdir(folder_path):
        result["error"] = f"路径不存在或不是目录: {folder_path}"
        return result
    try:
        total_bytes = 0
        video_files = []
        file_tree = []
        for root, dirs, files in os.walk(folder_path):
            rel_root = os.path.relpath(root, folder_path) if root != folder_path else "."
            for f in files:
                fp = os.path.join(root, f)
                try:
                    size = os.path.getsize(fp)
                    total_bytes += size
                    rel_path = os.path.join(rel_root, f)
                    if Path(f).suffix.lower() in VIDEO_EXTENSIONS:
                        video_files.append({"name": f, "path": rel_path, "size": size})
                    file_tree.append({"path": rel_path, "size": size})
                except OSError:
                    pass
        result["total_bytes"] = total_bytes
        result["total_size_gb"] = total_bytes / (1024 ** 3)
        result["estimated_minutes"] = result["total_size_gb"]  # 1G = 1分钟
        mins = result["estimated_minutes"]
        if mins >= 60:
            result["estimated_duration_str"] = f"{mins/60:.1f} 小时"
        else:
            result["estimated_duration_str"] = f"{mins:.1f} 分钟"
        result["video_files"] = sorted(video_files, key=lambda x: x["name"])
        result["file_tree"] = sorted(file_tree, key=lambda x: x["path"])
        result["location_name"] = os.path.basename(folder_path) or os.path.basename(os.path.dirname(folder_path))
        result["valid"] = True
    except Exception as e:
        result["error"] = str(e)
    return result

def format_size(size_bytes: int) -> str:
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.2f} GB"
    elif size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    return f"{size_bytes} B"

# ==========================================
# 2. 数据库逻辑
# ==========================================
DB_FILE = 'labeling_data_v2.db'

def _needs_migration(conn) -> bool:
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dataset'")
    if not c.fetchone():
        return False
    info = c.execute("PRAGMA table_info(dataset)").fetchall()
    cols = [x[1] for x in info]
    return "video_name" not in cols

def _ensure_road_surface_override_column(conn):
    c = conn.cursor()
    c.execute("PRAGMA table_info(dataset)")
    cols = [x[1] for x in c.fetchall()]
    if "has_road_surface_override" not in cols:
        conn.execute("ALTER TABLE dataset ADD COLUMN has_road_surface_override INTEGER DEFAULT 0")
        conn.commit()

def _ensure_duration_column(conn):
    c = conn.cursor()
    c.execute("PRAGMA table_info(dataset)")
    cols = [x[1] for x in c.fetchall()]
    if "duration" not in cols:
        conn.execute("ALTER TABLE dataset ADD COLUMN duration REAL")
        conn.commit()

def size_to_duration_minutes(size_bytes: int) -> float:
    """按 1G=1分钟 换算视频时长（分钟）"""
    return size_bytes / (1024 ** 3)

# 无视频地点标注的 folder_path 前缀，格式 MANUAL/{city}/{year}/{location_name}
MANUAL_LOCATION_PREFIX = "MANUAL"


def save_manual_location(data: dict) -> bool:
    """保存无视频的地点标注，folder_path=MANUAL/{city}/{year}/{location_name}，video_name=MANUAL"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    city = str(data.get("city", "")).strip()
    year = str(data.get("collection_year", "")).strip()
    loc_name = str(data.get("location_name", "")).strip()
    if not loc_name or not city:
        conn.close()
        return False
    folder_path = f"{MANUAL_LOCATION_PREFIX}/{city}/{year}/{loc_name}"
    video_name = "MANUAL"
    collection_time = f"{year}年" if year else ""
    if data.get("collection_month"):
        collection_time += f"{data['collection_month']}月"
    if data.get("collection_day"):
        collection_time += f"{data['collection_day']}日"
    duration = float(data.get("duration_minutes", 0) or 0)
    sec = data.get("secondary_tags", {})
    # 光照需手动设置（无采集时间可解析）
    atm = sec.get("四、大气环境", {})
    if "4.3 光照" not in atm:
        atm["4.3 光照"] = {"来源": "自然光", "强度": "正常"}
    sec["四、大气环境"] = atm
    c.execute('''
        INSERT OR REPLACE INTO dataset (
            video_name, folder_path, location_name, label_time, collection_time,
            top_road_category, top_road_subcategory,
            secondary_tags_json, has_dynamic_override, has_atmosphere_override, has_road_surface_override,
            duration, quality_tags, comments
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        video_name, folder_path, loc_name, datetime.now(), collection_time,
        data.get("top_road_category", ""), data.get("top_road_subcategory", ""),
        json.dumps(sec, ensure_ascii=False), 0, 0, 0,
        duration, "", ""
    ))
    conn.commit()
    conn.close()
    return True

def init_db():
    conn = sqlite3.connect(DB_FILE)
    if _needs_migration(conn):
        conn.execute("ALTER TABLE dataset RENAME TO dataset_legacy")
        conn.commit()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS dataset (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_name TEXT NOT NULL,
            folder_path TEXT NOT NULL,
            location_name TEXT NOT NULL,
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
            comments TEXT,
            UNIQUE(folder_path, video_name)
        )
    ''')
    conn.commit()
    _ensure_road_surface_override_column(conn)
    _ensure_duration_column(conn)
    conn.close()

def save_location_batch(data, folder_path: str, video_names: list):
    """对地点批量打标签：将该地点的标签应用到所有视频文件，保留已有的单视频覆盖。采集时间从视频文件名自动解析，duration 按 1G=1分钟 换算。"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    now = datetime.now()
    base_sec = data['secondary_tags']
    video_size_map = {v["name"]: v["size"] for v in data.get("video_files", [])}
    for vn in video_names:
        row = c.execute(
            "SELECT secondary_tags_json, has_dynamic_override, has_atmosphere_override, has_road_surface_override FROM dataset WHERE folder_path=? AND video_name=?",
            (folder_path, vn)
        ).fetchone()
        has_dyn, has_atm, has_road = 0, 0, 0
        if row and (row[1] or row[2] or row[3]):
            old_sec = json.loads(row[0]) if row[0] else {}
            sec = copy.deepcopy(base_sec)
            if row[1]:  # 保留动态目标覆盖
                sec["三、动态目标 (路面状况)"] = old_sec.get("三、动态目标 (路面状况)", {})
                has_dyn = 1
            if row[2]:  # 保留大气环境覆盖
                sec["四、大气环境"] = old_sec.get("四、大气环境", {})
                has_atm = 1
            if row[3]:  # 保留道路表面覆盖
                road_env = sec.get("一、道路静态环境", {})
                road_env["1.2 道路表面"] = old_sec.get("一、道路静态环境", {}).get("1.2 道路表面", {})
                sec["一、道路静态环境"] = road_env
                has_road = 1
        else:
            sec = base_sec
        # 根据采集时间自动设置 4.3 光照（日落/天黑时间由界面设定）
        sunset_h = data.get("sunset_h", 19)
        sunset_m = data.get("sunset_m", 0)
        dark_h = data.get("dark_h", 19)
        dark_m = data.get("dark_m", 30)
        h, mi = parse_collection_hour_minute(vn)
        if h is not None and mi is not None:
            atm = sec.get("四、大气环境", {})
            atm["4.3 光照"] = infer_lighting_from_time(h, mi, sunset_h, sunset_m, dark_h, dark_m)
            sec["四、大气环境"] = atm
        coll_time = parse_collection_time_from_filename(vn)
        duration = size_to_duration_minutes(video_size_map.get(vn, 0))
        c.execute('''
            INSERT OR REPLACE INTO dataset (
                video_name, folder_path, location_name, label_time, collection_time,
                top_road_category, top_road_subcategory,
                secondary_tags_json, has_dynamic_override, has_atmosphere_override, has_road_surface_override,
                duration, quality_tags, comments
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            vn, folder_path, data['location_name'], now, coll_time,
            data['top_road_category'], data['top_road_subcategory'],
            json.dumps(sec, ensure_ascii=False), has_dyn, has_atm, has_road,
            duration, "", ""
        ))
    conn.commit()
    conn.close()

def save_video_override(folder_path: str, video_name: str, override_section: str, override_data: dict):
    """对单个视频覆盖 动态目标 / 大气环境 / 道路表面 的标签"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    row = c.execute(
        "SELECT secondary_tags_json, has_dynamic_override, has_atmosphere_override, has_road_surface_override FROM dataset WHERE folder_path=? AND video_name=?",
        (folder_path, video_name)
    ).fetchone()
    if not row:
        conn.close()
        return False
    sec = json.loads(row[0]) if row[0] else {}
    has_dyn = row[1] or 0
    has_atm = row[2] or 0
    has_road = row[3] if len(row) > 3 else 0
    if override_section == "三、动态目标 (路面状况)":
        sec["三、动态目标 (路面状况)"] = override_data
        has_dyn = 1
    elif override_section == "四、大气环境":
        sec["四、大气环境"] = override_data
        has_atm = 1
    elif override_section == "1.2 道路表面":
        road_env = sec.get("一、道路静态环境", {})
        road_env["1.2 道路表面"] = override_data
        sec["一、道路静态环境"] = road_env
        has_road = 1
    c.execute('''
        UPDATE dataset SET secondary_tags_json=?, has_dynamic_override=?, has_atmosphere_override=?, has_road_surface_override=?, label_time=?
        WHERE folder_path=? AND video_name=?
    ''', (json.dumps(sec, ensure_ascii=False), has_dyn, has_atm, has_road, datetime.now(), folder_path, video_name))
    conn.commit()
    conn.close()
    return True

# ==========================================
# 3. Streamlit UI 逻辑
# ==========================================

def _render_manual_location_form(st):
    """无视频地点标注表单：填写 location_name、city、采集年份、数据时长等"""
    st.subheader("📍 无视频地点标注")
    st.caption("适用于无视频文件、无文件夹路径的地点，需手动填写关键信息，统计时参与时长汇总")
    
    common_cities = ["深圳", "长春", "香港", "北京", "上海", "广州", "杭州", "武汉", "成都"]

    st.markdown("### 📍 道路类型 (顶层)")
    c_top1, c_top2 = st.columns(2)
    def _on_manual_top_cat_change():
        top = st.session_state.get("manual_top_cat")
        if top and top in TOP_LEVEL_CONFIG:
            opts = TOP_LEVEL_CONFIG[top]
            if opts:
                st.session_state[f"manual_top_sub_{top}"] = opts[0]
    with c_top1:
        top_cat = st.selectbox(
            "顶层主类",
            options=list(TOP_LEVEL_CONFIG.keys()),
            key="manual_top_cat",
            on_change=_on_manual_top_cat_change,
        )
    with c_top2:
        sub_options = TOP_LEVEL_CONFIG[top_cat]
        top_sub_cat = st.selectbox("具体子类", options=sub_options, key=f"manual_top_sub_{top_cat}")
    
    st.divider()

    with st.form("manual_location_form"):
        st.markdown("### 📍 基础信息（必填）")
        loc_name = st.text_input("地点名称 *", placeholder="例: ADS_WZY_22")
        city_sel = st.selectbox("城市 *", options=[""] + common_cities + ["（手动输入）"], key="manual_city")
        city_input = st.text_input("城市（手动输入时填写）", placeholder="选择「手动输入」时在此填写", key="manual_city_input")
        city = city_input.strip() if city_sel == "（手动输入）" else city_sel
        col_yr, col_mo, col_dy = st.columns(3)
        with col_yr:
            col_year = st.number_input("采集年份 *", min_value=0, max_value=2100, value=datetime.now().year, key="manual_year")
        with col_mo:
            col_month = st.number_input("采集月份", min_value=0, max_value=12, value=0, key="manual_month", help="0 表示不填")
        with col_dy:
            col_day = st.number_input("采集日期", min_value=0, max_value=31, value=0, key="manual_day", help="0 表示不填")
        duration_minutes = st.number_input("数据时长 (分钟) *", min_value=0.0, value=60.0, step=1.0, key="manual_duration", help="该地点数据总时长，用于统计")
        
        st.divider()
        st.markdown("### 🌳 场景次级属性")
        tabs_names = list(SECONDARY_LEVEL_CONFIG.keys())
        tabs = st.tabs(tabs_names)
        secondary_results = {}
        for i, tab_name in enumerate(tabs_names):
            with tabs[i]:
                current_category = SECONDARY_LEVEL_CONFIG[tab_name]
                secondary_results[tab_name] = {}
                for sub_item, attributes in current_category.items():
                    with st.expander(f"{sub_item}", expanded=False):
                        cols = st.columns(len(attributes))
                        multi_fields = SECONDARY_MULTI_SELECT.get(sub_item, [])
                        for idx, (attr_name, options) in enumerate(attributes.items()):
                            if attr_name in multi_fields:
                                choices = cols[idx].multiselect(f"{attr_name}", options=options, key=f"manual_{tab_name}_{sub_item}_{attr_name}")
                                if choices:
                                    secondary_results[tab_name][sub_item] = secondary_results[tab_name].get(sub_item, {})
                                    secondary_results[tab_name][sub_item][attr_name] = choices
                            else:
                                choice = cols[idx].selectbox(f"{attr_name}", ["未标注"] + options, key=f"manual_{tab_name}_{sub_item}_{attr_name}")
                                if choice != "未标注":
                                    secondary_results[tab_name][sub_item] = secondary_results[tab_name].get(sub_item, {})
                                    secondary_results[tab_name][sub_item][attr_name] = choice
        
        submit = st.form_submit_button("💾 保存无视频地点标签")
        if submit:
            city_val = city.strip() if city else ""
            if not loc_name or not loc_name.strip():
                st.error("请输入地点名称")
            elif not city_val:
                st.error("请选择城市或选择「手动输入」后填写")
            else:
                data = {
                    "location_name": loc_name.strip(),
                    "city": str(city_val).strip(),
                    "collection_year": str(col_year) if col_year else "",
                    "collection_month": col_month if col_month else None,
                    "collection_day": col_day if col_day else None,
                    "duration_minutes": duration_minutes,
                    "top_road_category": top_cat,
                    "top_road_subcategory": top_sub_cat,
                    "secondary_tags": secondary_results,
                }
                if save_manual_location(data):
                    st.success(f"✅ 已保存地点「{loc_name}」的标注")
                else:
                    st.error("保存失败，请检查必填项")


def main():
    st.set_page_config(page_title="航测视频标注系统", layout="wide")
    init_db()

    # CSS 样式微调
    st.markdown("""
        <style>
        .stTabs [data-baseweb="tab-list"] { gap: 10px; }
        .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; }
        </style>
    """, unsafe_allow_html=True)

    st.title("🏷️ 航测数据标注系统")

    mode_tab1, mode_tab2 = st.tabs(["📁 有视频标注", "📍 无视频地点标注"])

    with mode_tab1:
        st.subheader("1. 文件夹路径与信息")
        folder_path = st.text_input(
            "📂 数据文件夹路径 *",
            placeholder=r"/mnt/nas/ADSafety/ADSafety/Aerial_Raw_Videos/2025-Changchun-AerialVideo-V1/ADS_1_1/",
            help="输入地点所有数据保存的文件夹地址，系统将自动扫描视频文件并估算总时长 (按 1G/分钟 换算)",
            key="folder_path_video",
        )
        folder_info = None
        if folder_path:
            folder_info = get_folder_info(folder_path)
            if folder_info["error"]:
                st.error(folder_info["error"])
            elif folder_info["valid"]:
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.metric("📦 文件夹总大小", format_size(folder_info["total_bytes"]))
                with c2:
                    st.metric("⏱️ 估算总时长 (1G/分钟)", folder_info["estimated_duration_str"])
                with c3:
                    st.metric("🎬 视频文件数", len(folder_info["video_files"]))
                st.info(f"📍 地点名称: **{folder_info['location_name']}** (从路径自动提取)")
                with st.expander("📁 文件目录结构", expanded=True):
                    if folder_info["file_tree"]:
                        tree_df = pd.DataFrame(folder_info["file_tree"])
                        tree_df.columns = ["相对路径", "大小(字节)"]
                        tree_df["大小"] = tree_df["大小(字节)"].apply(format_size)
                        st.dataframe(tree_df[["相对路径", "大小"]], width='content')
                    else:
                        st.caption("该目录下无文件")
        
        st.divider()
        st.subheader("2. 属性标注 (地点级批量)")

        with st.expander("🌅 光照自动标注设置", expanded=True):
            st.caption("根据视频采集时间自动设置 4.3 光照：日落前=正常，日落至天黑=昏暗，天黑后=黑暗")
            c_sun, c_dark = st.columns(2)
            with c_sun:
                sunset_t = st.time_input("日落时间", value=time(19, 0), key="sunset_time")
                sunset_h, sunset_m = sunset_t.hour, sunset_t.minute
            with c_dark:
                dark_t = st.time_input("天黑时间", value=time(19, 30), key="dark_time")
                dark_h, dark_m = dark_t.hour, dark_t.minute
        
        st.markdown("### 📍 道路类型 (顶层)")
        c_top1, c_top2 = st.columns(2)
        def _on_top_cat_change():
            top = st.session_state.get("top_main_cat")
            if top and top in TOP_LEVEL_CONFIG:
                opts = TOP_LEVEL_CONFIG[top]
                if opts:
                    st.session_state[f"top_sub_{top}"] = opts[0]
        top_cat = c_top1.selectbox(
            "顶层主类",
            options=list(TOP_LEVEL_CONFIG.keys()),
            key="top_main_cat",
            on_change=_on_top_cat_change,
        )
        sub_options = TOP_LEVEL_CONFIG[top_cat]
        top_sub_cat = c_top2.selectbox("具体子类", options=sub_options, key=f"top_sub_{top_cat}")
        
        st.divider()

        with st.form("advanced_label_form"):
            st.markdown("### 📍 基础信息")
            loc_name = st.text_input("地点名称 (可修改)", value=folder_info["location_name"] if folder_info and folder_info.get("valid") else "", placeholder="例: ADS_1_1")

            st.divider()

            st.markdown("### 🌳 场景次级属性 (对地点打标签后，将应用到该文件夹内所有视频)")
            tabs_names = list(SECONDARY_LEVEL_CONFIG.keys())
            tabs = st.tabs(tabs_names)
            secondary_results = {}

            for i, tab_name in enumerate(tabs_names):
                with tabs[i]:
                    current_category = SECONDARY_LEVEL_CONFIG[tab_name]
                    secondary_results[tab_name] = {}
                    for sub_item, attributes in current_category.items():
                        with st.expander(f"{sub_item}", expanded=False):
                            cols = st.columns(len(attributes))
                            multi_fields = SECONDARY_MULTI_SELECT.get(sub_item, [])
                            for idx, (attr_name, options) in enumerate(attributes.items()):
                                if attr_name in multi_fields:
                                    choices = cols[idx].multiselect(
                                        f"{attr_name}", options=options,
                                        key=f"{tab_name}_{sub_item}_{attr_name}"
                                    )
                                    if choices:
                                        if sub_item not in secondary_results[tab_name]:
                                            secondary_results[tab_name][sub_item] = {}
                                        secondary_results[tab_name][sub_item][attr_name] = choices
                                else:
                                    choice = cols[idx].selectbox(
                                        f"{attr_name}", ["未标注"] + options,
                                        key=f"{tab_name}_{sub_item}_{attr_name}"
                                    )
                                    if choice != "未标注":
                                        if sub_item not in secondary_results[tab_name]:
                                            secondary_results[tab_name][sub_item] = {}
                                        secondary_results[tab_name][sub_item][attr_name] = choice

            submit_btn = st.form_submit_button("💾 保存地点标签并应用到所有视频", type="primary")

            if submit_btn:
                if not folder_path or not folder_path.strip():
                    st.error("请输入文件夹路径")
                elif not folder_info or not folder_info.get("valid"):
                    st.error("请确认文件夹路径有效")
                elif not folder_info.get("video_files"):
                    st.error("该文件夹内没有视频文件")
                else:
                    loc = loc_name.strip() or folder_info["location_name"]
                    final_data = {
                        "location_name": loc,
                        "top_road_category": top_cat,
                        "top_road_subcategory": top_sub_cat,
                        "secondary_tags": secondary_results,
                        "video_files": folder_info["video_files"],
                        "sunset_h": sunset_h, "sunset_m": sunset_m,
                        "dark_h": dark_h, "dark_m": dark_m,
                    }
                    video_names = [v["name"] for v in folder_info["video_files"]]
                    save_location_batch(final_data, folder_info["path"], video_names)
                    st.success(f"✅ 已将标签应用到 {len(video_names)} 个视频文件！")
                    with st.expander("查看生成的 JSON 数据结构"):
                        st.json(final_data)

        st.divider()
        st.subheader("3. 单视频标签覆盖 (采集日期时间 / 道路表面 / 动态目标 / 大气环境)")
        st.caption("采集日期时间从视频文件名自动解析；可对单个视频单独修改「道路表面」「动态目标」或「大气环境」")
        
        if folder_info and folder_info.get("valid") and folder_info.get("video_files"):
            override_tab1, override_tab2 = st.tabs(["选择视频并覆盖", "说明"])
            with override_tab1:
                video_list = [v["name"] for v in folder_info["video_files"]]
                sel_video = st.selectbox("选择要单独修改标签的视频", options=video_list)
                coll_time_str = parse_collection_time_from_filename(sel_video)
                if coll_time_str:
                    st.info(f"📅 **采集日期时间**（从文件名解析）: {coll_time_str}")
                else:
                    st.caption("采集日期时间: 无法从文件名解析")
                override_section = st.radio(
                    "覆盖的标签类别",
                    ["1.2 道路表面", "三、动态目标 (路面状况)", "四、大气环境"]
                )
                if override_section == "1.2 道路表面":
                    current_cat = {"1.2 道路表面": SECONDARY_LEVEL_CONFIG["一、道路静态环境"]["1.2 道路表面"]}
                else:
                    current_cat = SECONDARY_LEVEL_CONFIG[override_section]
                override_results = {}
                for sub_item, attributes in current_cat.items():
                    with st.expander(sub_item):
                        cols = st.columns(len(attributes))
                        multi_fields = SECONDARY_MULTI_SELECT.get(sub_item, [])
                        for idx, (attr_name, options) in enumerate(attributes.items()):
                            if attr_name in multi_fields:
                                choices = cols[idx].multiselect(
                                    f"{attr_name}", options=options,
                                    key=f"override_{override_section}_{sub_item}_{attr_name}"
                                )
                                if choices:
                                    if sub_item not in override_results:
                                        override_results[sub_item] = {}
                                    override_results[sub_item][attr_name] = choices
                            else:
                                choice = cols[idx].selectbox(
                                    f"{attr_name}", ["未标注"] + options,
                                    key=f"override_{override_section}_{sub_item}_{attr_name}"
                                )
                                if choice != "未标注":
                                    if sub_item not in override_results:
                                        override_results[sub_item] = {}
                                    override_results[sub_item][attr_name] = choice
                to_save = override_results.get("1.2 道路表面", override_results) if override_section == "1.2 道路表面" else override_results
                if st.button("💾 保存该视频的覆盖标签"):
                    if to_save:
                        ok = save_video_override(folder_info["path"], sel_video, override_section, to_save)
                        if ok:
                            st.success(f"已更新视频 {sel_video} 的 {override_section} 标签")
                        else:
                            st.warning("该视频尚无地点级标签，请先完成「2. 属性标注」并保存地点标签")
                    else:
                        st.warning("请至少选择一个标签")
            with override_tab2:
                st.markdown("""
                - **采集日期时间**：从视频文件名自动解析（如 DJI_20250711155546_0001_V → 2025年7月11日 15:55）
                - **地点级标签**：对地点（如 ADS_1_1）打标签后，该文件夹内所有视频默认使用相同标签
                - **单视频覆盖**：针对「道路表面」「动态目标」「大气环境」，若某个视频与地点整体不同，可在此单独修改
                - **数据库统计**：以视频文件为单位存储，便于按视频维度统计与导出
                """)

    with mode_tab2:
        _render_manual_location_form(st)

    # --- 数据预览 ---
    st.markdown("---")
    if st.checkbox("🔍 查看数据库记录"):
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql("SELECT id, video_name, folder_path, location_name, label_time, duration, has_dynamic_override, has_atmosphere_override FROM dataset ORDER BY id DESC LIMIT 20", conn)
        if "duration" in df.columns:
            df["duration"] = df["duration"].apply(lambda x: f"{x:.2f} 分钟" if pd.notna(x) else "")
        st.dataframe(df, use_container_width=True)
        conn.close()

if __name__ == "__main__":
    main()