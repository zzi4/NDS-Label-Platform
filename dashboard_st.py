"""
ODD 人工标注平台 v3
- 视觉风格：完全对齐「驭研科技大规模自然驾驶数据集统计平台」截图
  · 深蓝渐变 Header（扫光动画 + tech bracket）
  · KPI 卡片对齐截图四色渐变（蓝/靛/青绿/紫）+ 展开条
  · 侧边栏深蓝科技主题
- 功能同 v2，代码优化：
  · 统一 DB 上下文管理器，消除重复连接
  · 修复 time_period 逻辑（原版 h<7 未归入夜间）
  · _field 签名简化，去掉无用参数 schema_dict
  · 去掉未使用的 import shutil / parse_time

运行：conda activate nds && streamlit run label_platform_v3.py
"""
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

# ─── 路径常量 ─────────────────────────────────────────────────────────
_BASE = Path(__file__).parent
INDEX_FILE   = _BASE / "sessions_index.json"
DB_FILE      = str(_BASE / "label_platform.db")
AUTO_DB_FILE = str(_BASE / "auto_labeled.db")

# ─── 配色 ─────────────────────────────────────────────────────────────
BLUE = ["#1a56a8","#059669","#d97706","#7c3aed",
        "#0891b2","#dc2626","#0d9488","#f59e0b","#6366f1","#16a34a"]

# ─── 城市映射 ─────────────────────────────────────────────────────────
CITY_MAP = {
    "ShenZhen":"深圳","Changchun":"长春","Hongkong":"香港",
    "Beijing":"北京","Shanghai":"上海","Guangzhou":"广州",
    "Haerbin":"哈尔滨","Hangzhou":"杭州","Wuhan":"武汉","Chengdu":"成都",
}

# ─── 标签体系 ─────────────────────────────────────────────────────────
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

# ─── 工具函数 ─────────────────────────────────────────────────────────
def extract_city(l1: str) -> str:
    m = re.search(r"\d{4}-([A-Za-z]+)-AerialVideo", l1)
    return CITY_MAP.get(m.group(1), m.group(1)) if m else l1

def parse_hour(ct: str) -> int:
    m = re.search(r"\s(\d{1,2}):", ct or "")
    return int(m.group(1)) if m else -1

def time_period(h: int) -> str:
    """修复原版：h<0 未知；0-6 / 19-23 均归夜间"""
    if h < 0:        return "未知"
    if 7 <= h < 9:   return "早高峰 (07-09)"
    if 17 <= h < 19: return "晚高峰 (17-19)"
    if h >= 19 or h < 7: return "夜间 (19-07)"
    return "日常规"

def fmt_dur(minutes: float) -> str:
    return f"{minutes/60:.1f}h" if minutes >= 60 else f"{minutes:.0f}min"

def extract_tag(d: dict, sec: str, sub: str, attr: str):
    try:
        v = d[sec][sub][attr]
        return v if isinstance(v, list) else ([v] if v else None)
    except (KeyError, TypeError):
        return None

# ─── 数据库上下文管理器 ───────────────────────────────────────────────
@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_FILE)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

# ─── 数据库初始化 ─────────────────────────────────────────────────────
def init_db():
    with db_conn() as conn:
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

# ─── 数据加载（带缓存） ───────────────────────────────────────────────
def _resolve_img(path: str) -> str:
    """将路径解析为基于脚本目录的绝对路径；绝对路径不存在时尝试取文件名在 _BASE 下查找"""
    if not path:
        return path
    p = Path(path)
    if not p.is_absolute():
        return str(_BASE / p)
    if p.exists():
        return str(p)
    # 绝对路径在当前环境不存在，依次在已知子目录下按文件名查找
    for subdir in ("frames", "photo", ""):
        candidate = _BASE / subdir / p.name if subdir else _BASE / p.name
        if candidate.exists():
            return str(candidate)
    return str(p)

@st.cache_data(ttl=60)
def load_df() -> pd.DataFrame:
    SQL = ("SELECT id,video_name,folder_path,location_name,collection_time,"
           "top_road_category,top_road_subcategory,secondary_tags_json,duration,image_path "
           "FROM dataset")
    frames = []
    try:
        with db_conn() as conn:
            df_manual = pd.read_sql(SQL, conn)
            df_manual["source"] = "手动标注"
            frames.append(df_manual)
    except Exception:
        pass
    try:
        conn2 = sqlite3.connect(AUTO_DB_FILE)
        df_auto = pd.read_sql(SQL, conn2)
        conn2.close()
        df_auto["source"] = "VLM自动"
        frames.append(df_auto)
    except Exception:
        pass
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    # 若同一 folder_path 手动和 VLM 都有，优先保留手动标注
    df = df.sort_values("source").drop_duplicates(subset=["folder_path"], keep="first")
    # 用 sessions_index.json 补全缺失的 image_path
    try:
        if INDEX_FILE.exists():
            with open(INDEX_FILE, encoding="utf-8") as _f:
                _idx = json.load(_f)
            _fp2img = {s["folder_path"]: _resolve_img(s["image_path"])
                       for s in _idx.get("sessions", [])
                       if s.get("image_path")}
            _mask = df["image_path"].isna() | (df["image_path"] == "") | \
                    df["image_path"].apply(lambda p: bool(p) and not Path(str(p)).exists())
            df.loc[_mask, "image_path"] = df.loc[_mask, "folder_path"].map(_fp2img)
    except Exception:
        pass
    # 第二层兜底：对所有路径仍不存在的行（含 PHOTO 地点），用 _resolve_img 在 frames/ 下查找
    df["image_path"] = df["image_path"].apply(
        lambda p: _resolve_img(str(p)) if pd.notna(p) and p else p
    )
    def _city(p):
        if not p or "/" not in p: return "手动录入"
        prefix = p.split("/")[0]
        if prefix in ("MANUAL", "PHOTO"):
            parts = p.split("/")
            return parts[1] if len(parts) > 1 else "手动录入"
        return extract_city(prefix)
    df["city"] = df["folder_path"].apply(_city)
    df["year"] = df["folder_path"].apply(lambda p: p[:4] if p and p[:4].isdigit() else "")
    df["_tags"] = df["secondary_tags_json"].apply(
        lambda s: json.loads(s) if pd.notna(s) and s else {}
    )
    return df

# ─── Plotly 公共布局参数 ──────────────────────────────────────────────
def _cl() -> dict:
    return dict(
        plot_bgcolor="rgba(255,255,255,0)",
        paper_bgcolor="rgba(255,255,255,0.55)",
        title_font=dict(size=14, color="#0d2d5e", family="'Helvetica Neue',Arial,sans-serif"),
        title_x=0.02,
        font=dict(family="'Helvetica Neue','PingFang SC',Arial,sans-serif", size=12, color="#2d3748"),
        xaxis=dict(showgrid=False, showline=False,
                   tickfont=dict(size=11, color="#4a6fa5"), tickcolor="rgba(0,0,0,0)"),
        yaxis=dict(gridcolor="rgba(160,174,192,0.22)", gridwidth=1,
                   showline=False, zeroline=False, tickfont=dict(size=11, color="#4a6fa5")),
        legend=dict(bgcolor="rgba(255,255,255,0.65)",
                    bordercolor="rgba(99,179,237,0.25)", borderwidth=1,
                    font=dict(size=11, color="#2d3748")),
        hoverlabel=dict(bgcolor="rgba(255,255,255,0.92)", font_size=12,
                        font_color="#0d2d5e", bordercolor="rgba(99,179,237,0.4)"),
        coloraxis_showscale=False,
    )


# ─── 页面 2：统计看板 ─────────────────────────────────────────────────
def page_dashboard():
    import plotly.express as px

    df = load_df()
    if df.empty:
        st.info("数据库暂无数据，请先完成标注"); return

    with st.expander("🔍 筛选条件", expanded=True):
        _dc1, _dc2, _dc3 = st.columns([1, 2, 2])
        with _dc1:
            if st.button("🔄 刷新数据", key="dash_refresh"):
                load_df.clear(); st.rerun()
        with _dc2:
            sel_c = st.multiselect("城市", sorted(df["city"].dropna().unique()),
                                   default=sorted(df["city"].dropna().unique()), key="dc")
        with _dc3:
            sel_r = st.multiselect("道路主类", sorted(df["top_road_category"].dropna().unique()),
                                   default=sorted(df["top_road_category"].dropna().unique()), key="dr")

    # ── 高级筛选 ──
    def _tag_vals(col_df, sec, sub, attr):
        vals = set()
        for d in col_df["_tags"]:
            v = extract_tag(d, sec, sub, attr)
            if v:
                vals.update(v)
        return sorted(vals)

    PERIOD_ORDER = ["早高峰 (07-09)", "日常规", "晚高峰 (17-19)", "夜间 (19-07)", "未知"]

    with st.expander("⚙️ 高级筛选", expanded=False):
        # ── 一、道路静态环境 ──────────────────────────────────────
        st.markdown("**一、道路静态环境**")
        _r1c1, _r1c2, _r1c3, _r1c4 = st.columns(4)
        _r2c1, _r2c2, _r2c3, _r2c4 = st.columns(4)

        sub_opts    = sorted(df["top_road_subcategory"].dropna().unique())
        cross_opts  = _tag_vals(df, "一、道路静态环境", "1.6 道路交叉",       "交叉类型")
        lane_n_opts = _tag_vals(df, "一、道路静态环境", "1.4 包含车道特征",   "最宽车道数量")
        lane_t_opts = _tag_vals(df, "一、道路静态环境", "1.4 包含车道特征",   "车道类型")
        lane_w_opts = _tag_vals(df, "一、道路静态环境", "1.4 包含车道特征",   "车道宽度")
        surf_opts   = _tag_vals(df, "一、道路静态环境", "1.2 道路表面",       "表面类型")
        surf_s_opts = _tag_vals(df, "一、道路静态环境", "1.2 道路表面",       "表面状态")
        slope_opts  = _tag_vals(df, "一、道路静态环境", "1.3 道路几何",       "坡度")
        curv_opts   = _tag_vals(df, "一、道路静态环境", "1.3 道路几何",       "曲率")
        bank_opts   = _tag_vals(df, "一、道路静态环境", "1.3 道路几何",       "横坡")
        edge_opts   = _tag_vals(df, "一、道路静态环境", "1.5 道路边缘",       "边缘类型")

        with _r1c1: sel_sub      = st.multiselect("道路子类",   sub_opts,    default=sub_opts,    key="adv_sub")
        with _r1c2: sel_cross    = st.multiselect("交叉类型",   cross_opts,  default=cross_opts,  key="adv_cross")
        with _r1c3: sel_lane_n   = st.multiselect("车道数量",   lane_n_opts, default=lane_n_opts, key="adv_lane_n")
        with _r1c4: sel_lane_t   = st.multiselect("车道类型",   lane_t_opts, default=lane_t_opts, key="adv_lane_t")
        with _r2c1: sel_lane_w   = st.multiselect("车道宽度",   lane_w_opts, default=lane_w_opts, key="adv_lane_w")
        with _r2c2: sel_surf     = st.multiselect("路面类型",   surf_opts,   default=surf_opts,   key="adv_surf")
        with _r2c3: sel_surf_s   = st.multiselect("路面状态",   surf_s_opts, default=surf_s_opts, key="adv_surf_s")
        with _r2c4: sel_edge     = st.multiselect("道路边缘",   edge_opts,   default=edge_opts,   key="adv_edge")

        _r3c1, _r3c2, _r3c3 = st.columns(3)
        with _r3c1: sel_slope    = st.multiselect("坡度",       slope_opts,  default=slope_opts,  key="adv_slope")
        with _r3c2: sel_curv     = st.multiselect("曲率",       curv_opts,   default=curv_opts,   key="adv_curv")
        with _r3c3: sel_bank     = st.multiselect("横坡",       bank_opts,   default=bank_opts,   key="adv_bank")

        st.divider()
        # ── 二、交通设施 ──────────────────────────────────────────
        st.markdown("**二、交通设施**")
        _r4c1, _r4c2, _r4c3, _r4c4 = st.columns(4)

        sig_opts    = _tag_vals(df, "二、交通设施", "2.1 交通控制",       "信号灯")
        sign_opts   = _tag_vals(df, "二、交通设施", "2.1 交通控制",       "标志牌")
        mark_opts   = _tag_vals(df, "二、交通设施", "2.1 交通控制",       "地面标签")
        fac_opts    = _tag_vals(df, "二、交通设施", "2.2 路侧与周边环境", "设施")
        spec_opts   = _tag_vals(df, "二、交通设施", "2.3 特殊设施",       "类型")

        with _r4c1: sel_sig      = st.multiselect("信号灯",     sig_opts,   default=sig_opts,   key="adv_sig")
        with _r4c2: sel_sign     = st.multiselect("标志牌",     sign_opts,  default=sign_opts,  key="adv_sign")
        with _r4c3: sel_mark     = st.multiselect("地面标线",   mark_opts,  default=mark_opts,  key="adv_mark")
        with _r4c4: sel_spec     = st.multiselect("特殊设施",   spec_opts,  default=spec_opts,  key="adv_spec")
        sel_fac = st.multiselect("路侧设施", fac_opts, default=fac_opts, key="adv_fac")

        st.divider()
        # ── 三、动态目标 ──────────────────────────────────────────
        st.markdown("**三、动态目标**")
        _r5c1, _r5c2, _r5c3 = st.columns(3)

        mv_opts     = _tag_vals(df, "三、动态目标 (路面状况)", "3.1 机动车",   "类型")
        vru_opts    = _tag_vals(df, "三、动态目标 (路面状况)", "3.2 VRU",      "类型")
        obs_opts    = _tag_vals(df, "三、动态目标 (路面状况)", "3.4 障碍物",   "类型")

        with _r5c1: sel_mv       = st.multiselect("机动车类型", mv_opts,    default=mv_opts,    key="adv_mv")
        with _r5c2: sel_vru      = st.multiselect("VRU 类型",   vru_opts,   default=vru_opts,   key="adv_vru")
        with _r5c3: sel_obs      = st.multiselect("障碍物",     obs_opts,   default=obs_opts,   key="adv_obs")

        st.divider()
        # ── 四、大气环境 + 时段 ───────────────────────────────────
        st.markdown("**四、大气环境 & 采集时段**")
        _r6c1, _r6c2, _r6c3, _r6c4, _r6c5 = st.columns(5)

        wth_opts    = _tag_vals(df, "四、大气环境", "4.1 天气",  "类型")
        light_opts  = _tag_vals(df, "四、大气环境", "4.3 光照",  "来源")
        lumi_opts   = _tag_vals(df, "四、大气环境", "4.3 光照",  "强度")
        temp_opts   = _tag_vals(df, "四、大气环境", "4.4 气温",  "估算")

        with _r6c1: sel_wth      = st.multiselect("天气",       wth_opts,   default=wth_opts,   key="adv_wth")
        with _r6c2: sel_light    = st.multiselect("光照来源",   light_opts, default=light_opts, key="adv_light")
        with _r6c3: sel_lumi     = st.multiselect("光照强度",   lumi_opts,  default=lumi_opts,  key="adv_lumi")
        with _r6c4: sel_temp     = st.multiselect("气温",       temp_opts,  default=temp_opts,  key="adv_temp")
        with _r6c5: sel_period   = st.multiselect("采集时段",   PERIOD_ORDER, default=PERIOD_ORDER, key="adv_period")

    def _tag_match(tags, sec, sub, attr, allowed):
        v = extract_tag(tags, sec, sub, attr)
        if not v:
            return True   # 无标签的不过滤掉
        return bool(set(v) & set(allowed))

    df["_period"] = df["collection_time"].apply(lambda ct: time_period(parse_hour(ct)))

    df_f = df[
        df["city"].isin(sel_c) &
        df["top_road_category"].isin(sel_r) &
        (df["top_road_subcategory"].isna() | df["top_road_subcategory"].isin(sel_sub)) &
        df["_period"].isin(sel_period) &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.6 道路交叉",       "交叉类型",   sel_cross))  &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.4 包含车道特征",   "最宽车道数量", sel_lane_n)) &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.4 包含车道特征",   "车道类型",   sel_lane_t)) &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.4 包含车道特征",   "车道宽度",   sel_lane_w)) &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.2 道路表面",       "表面类型",   sel_surf))   &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.2 道路表面",       "表面状态",   sel_surf_s)) &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.3 道路几何",       "坡度",       sel_slope))  &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.3 道路几何",       "曲率",       sel_curv))   &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.3 道路几何",       "横坡",       sel_bank))   &
        df["_tags"].apply(lambda d: _tag_match(d, "一、道路静态环境", "1.5 道路边缘",       "边缘类型",   sel_edge))   &
        df["_tags"].apply(lambda d: _tag_match(d, "二、交通设施",     "2.1 交通控制",       "信号灯",     sel_sig))    &
        df["_tags"].apply(lambda d: _tag_match(d, "二、交通设施",     "2.1 交通控制",       "标志牌",     sel_sign))   &
        df["_tags"].apply(lambda d: _tag_match(d, "二、交通设施",     "2.1 交通控制",       "地面标签",   sel_mark))   &
        df["_tags"].apply(lambda d: _tag_match(d, "二、交通设施",     "2.2 路侧与周边环境", "设施",       sel_fac))    &
        df["_tags"].apply(lambda d: _tag_match(d, "二、交通设施",     "2.3 特殊设施",       "类型",       sel_spec))   &
        df["_tags"].apply(lambda d: _tag_match(d, "三、动态目标 (路面状况)", "3.1 机动车",   "类型",       sel_mv))     &
        df["_tags"].apply(lambda d: _tag_match(d, "三、动态目标 (路面状况)", "3.2 VRU",      "类型",       sel_vru))    &
        df["_tags"].apply(lambda d: _tag_match(d, "三、动态目标 (路面状况)", "3.4 障碍物",   "类型",       sel_obs))    &
        df["_tags"].apply(lambda d: _tag_match(d, "四、大气环境",     "4.1 天气",           "类型",       sel_wth))    &
        df["_tags"].apply(lambda d: _tag_match(d, "四、大气环境",     "4.3 光照",           "来源",       sel_light))  &
        df["_tags"].apply(lambda d: _tag_match(d, "四、大气环境",     "4.3 光照",           "强度",       sel_lumi))   &
        df["_tags"].apply(lambda d: _tag_match(d, "四、大气环境",     "4.4 气温",           "估算",       sel_temp))
    ].copy()
    df_f["duration"] = df_f["duration"].fillna(0)

    # ── KPI 卡片 ──────────────────────────────────────────────────────
    n_labeled = len(df_f)
    n_cities  = df_f["city"].nunique()
    n_locs    = df_f["location_name"].nunique()
    hrs       = df_f["duration"].sum() / 60

    if "kpi_open" not in st.session_state:
        st.session_state["kpi_open"] = None

    kpi_defs = [
        ("kpi-blue",   "📹", f"{n_labeled:,}", "视频总数",      "videos"),
        ("kpi-indigo", "🌆", str(n_cities),    "覆盖城市",      "cities"),
        ("kpi-teal",   "⏱️",  f"{hrs:.1f}",    "总时长（小时）", None),
        ("kpi-purple", "📍", str(n_locs),      "地点数",        None),
    ]
    kpi_cols = st.columns(4)
    for col, (cls, icon, num, lbl, key) in zip(kpi_cols, kpi_defs):
        with col:
            st.markdown(
                f"<div class='kpi-card {cls}'>"
                f"<div class='kpi-icon'>{icon}</div>"
                f"<div class='kpi-num'>{num}</div>"
                f"<div class='kpi-lbl'>{lbl}</div>"
                f"</div>", unsafe_allow_html=True
            )
            if key:
                is_open = st.session_state["kpi_open"] == key
                if st.button("▾ 收起" if is_open else "▸ 展开详情",
                             key=f"kpi_btn_{key}", use_container_width=True):
                    st.session_state["kpi_open"] = None if is_open else key
                    st.rerun()
            else:
                st.markdown(
                    "<p style='text-align:center;font-size:0.76rem;color:#888;margin:6px 0'>含图片数据集</p>",
                    unsafe_allow_html=True
                )

    # ── 展开面板 ──────────────────────────────────────────────────────
    _open = st.session_state["kpi_open"]

    if _open == "videos":
        st.markdown("#### 📹 视频 / 场景列表")
        _cols = ["city", "location_name", "top_road_category", "top_road_subcategory",
                 "duration", "collection_time", "source"]
        disp = df_f[[c for c in _cols if c in df_f.columns]].copy()
        disp["duration"] = disp["duration"].apply(fmt_dur)
        disp = disp.rename(columns={
            "city": "城市", "location_name": "地点",
            "top_road_category": "道路主类", "top_road_subcategory": "道路子类",
            "duration": "时长", "collection_time": "采集时间", "source": "来源"
        })
        st.dataframe(disp, use_container_width=True, height=400)

    elif _open == "cities":
        st.markdown("#### 🌆 各城市概览")
        city_sum = (df_f.groupby("city")
                    .agg(场景数=("id", "count"), 总时长=("duration", "sum"))
                    .reset_index()
                    .assign(时长_h=lambda x: (x["总时长"] / 60).round(1))
                    .sort_values("场景数", ascending=False))
        n_c = min(4, len(city_sum))
        if n_c:
            c_cols = st.columns(n_c)
            for i, (_, r) in enumerate(city_sum.iterrows()):
                with c_cols[i % 4]:
                    st.markdown(
                        f"<div style='background:linear-gradient(135deg,#1a56a8,#0891b2);"
                        f"border-radius:12px;padding:16px 12px;color:#fff;"
                        f"margin-bottom:10px;text-align:center'>"
                        f"<div style='font-size:1.1rem;font-weight:800;margin-bottom:6px'>{r['city']}</div>"
                        f"<div style='font-size:0.82rem;opacity:.9'>⏱ {r['时长_h']}h</div>"
                        f"<div style='font-size:0.82rem;opacity:.9'>📹 {r['场景数']} 场景</div>"
                        f"</div>", unsafe_allow_html=True
                    )

    st.divider()

    # ── 地域与时间分布 ──
    st.markdown("### 📍 地域与时间分布")
    c1, c2 = st.columns(2)
    with c1:
        cd = df_f.groupby("city").agg(数量=("id","count")).reset_index()
        if not cd.empty:
            fig = px.bar(cd, x="city", y="数量", text="数量", color="city",
                         color_discrete_sequence=BLUE, title="各城市标注数量", labels={"city":"城市"})
            fig.update_traces(textposition="outside", textfont=dict(size=11, color="#0d2d5e"),
                              marker_line_width=0, opacity=0.88)
            fig.update_layout(**_cl(), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        ld = (df_f.groupby("location_name").agg(时长=("duration","sum")).reset_index()
              .assign(**{"时长(h)": lambda x: x["时长"] / 60})
              .sort_values("时长(h)", ascending=False).head(15))
        if not ld.empty:
            fig = px.bar(ld, x="location_name", y="时长(h)", color="location_name",
                         color_discrete_sequence=BLUE, title="各地点视频总时长 (Top 15)",
                         labels={"location_name":"地点","时长(h)":"时长(h)"})
            fig.update_traces(marker_line_width=0, opacity=0.88)
            fig.update_layout(**_cl(), showlegend=False, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    # ── 采集时段 ──
    st.markdown("### 🕐 采集时段分布")
    df_f["period"] = df_f["_period"]
    pd_df = df_f.groupby("period").size().reset_index(name="数量")
    pd_df["_o"] = pd_df["period"].apply(lambda x: PERIOD_ORDER.index(x) if x in PERIOD_ORDER else 99)
    pd_df = pd_df.sort_values("_o").drop(columns=["_o"])
    if not pd_df.empty:
        fig = px.bar(pd_df, x="period", y="数量", text="数量", color="period",
                     color_discrete_sequence=BLUE, title="采集时段分布", labels={"period":"时段"})
        fig.update_traces(textposition="outside", textfont=dict(size=11, color="#0d2d5e"),
                          marker_line_width=0, opacity=0.88)
        fig.update_layout(**_cl(), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # ── 道路类型旭日图 ──
    st.markdown("### 🛣️ 道路类型分布")
    top_df = df_f[df_f["top_road_category"].notna() & (df_f["top_road_category"] != "")]
    if not top_df.empty:
        rows = []
        for _, row in top_df.iterrows():
            vals = extract_tag(row["_tags"],"一、道路静态环境","1.6 道路交叉","交叉类型")
            for ct in (vals or ["未标注"]):
                rows.append({"主类":row["top_road_category"],
                             "子类":row.get("top_road_subcategory",""),"交叉类型":ct})
        if rows:
            sb = pd.DataFrame(rows).groupby(["主类","子类","交叉类型"]).size().reset_index(name="数量")
            fig = px.sunburst(sb, path=["主类","子类","交叉类型"], values="数量",
                              title="道路类型层级分布", color_discrete_sequence=BLUE)
            fig.update_traces(textfont=dict(size=11))
            fig.update_layout(**_cl(), margin=dict(t=52,l=0,r=0,b=0))
            st.plotly_chart(fig, use_container_width=True)

    # ── 标签分布 ──
    st.markdown("### 🏷️ 标签分布")
    tag_opts = [f"{sub} · {attr}" for _, sub, attr in TAG_PATHS]
    sel_tag  = st.selectbox("标签维度", tag_opts, key="dash_tag")
    sec, sub, attr = TAG_PATHS[tag_opts.index(sel_tag)]
    items = []
    for d in df_f["_tags"]:
        v = extract_tag(d, sec, sub, attr)
        if v: items.extend(v)
    if items:
        cnt = pd.Series(items).value_counts().reset_index()
        cnt.columns = ["标签值","数量"]
        ct = st.radio("图表类型", ["柱状图","饼图"], horizontal=True, key="dchart")
        if ct == "饼图":
            fig = px.pie(cnt, values="数量", names="标签值",
                         title=f"{sub} · {attr}", color_discrete_sequence=BLUE, hole=0.38)
            fig.update_traces(texttemplate="%{label}<br>%{percent:.1%}",
                              textposition="outside", textfont=dict(size=11), pull=[0.03]*len(cnt))
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

    # ── 交叉分析 ──
    st.markdown("### 📈 交叉分析")
    DIMS = [("city","城市"),("top_road_category","道路主类"),
            ("top_road_subcategory","道路子类"),("period","时段")]
    lbls, keys = [v for _,v in DIMS], [k for k,_ in DIMS]
    dc1, dc2 = st.columns(2)
    with dc1: s1 = st.selectbox("维度 1", lbls, key="cx1")
    with dc2: s2 = st.selectbox("维度 2", lbls, index=1, key="cx2")
    k1, k2 = keys[lbls.index(s1)], keys[lbls.index(s2)]
    if k1 != k2 and all(k in df_f.columns for k in (k1, k2)):
        pivot = df_f.groupby([k1, k2]).size().unstack(fill_value=0)
        st.dataframe(pivot.style.background_gradient(cmap="Blues"), use_container_width=True)

    with st.expander("📋 原始数据"):
        cols_show = ["source","folder_path","city","location_name","top_road_category",
                     "top_road_subcategory","collection_time","duration"]
        st.dataframe(df_f[[c for c in cols_show if c in df_f.columns]].head(200),
                     use_container_width=True)

# ─── CSS（完全对齐截图平台视觉风格） ────────────────────────────────
CSS = """
<style>
/* ══════════════════════════════════════════════════════
   驭研科技 ODD 标注平台 v3
   主色  #0d2d5e · 品牌  #1a56a8 · 亮色  #3b7dd8
   背景  #eef2f9
   完全对齐「自然驾驶数据集统计平台」截图风格
══════════════════════════════════════════════════════ */

/* ── 隐藏 Streamlit 原生工具栏 / 菜单 / 页脚，并清除 stMain 预留的顶部占位 ── */
header[data-testid="stHeader"]       { display:none !important; height:0 !important; }
[data-testid="stToolbar"]            { display:none !important; height:0 !important; }
[data-testid="stDecoration"]         { display:none !important; height:0 !important; }
#MainMenu                            { display:none !important; }
footer                               { display:none !important; }
/* 清除 Streamlit 给工具栏预留的 padding-top */
section[data-testid="stMain"]        { padding-top: 0 !important; }
section[data-testid="stMain"] > div:first-child { padding-top: 0 !important; }

/* ── 全局背景 ── */
.stApp { background: #eef2f9 !important; }

/* ── 主内容卡片 ── */
.block-container {
    background: rgba(255,255,255,0.97) !important;
    border-radius: 16px !important;
    padding: 0 2rem 2.4rem !important;
    box-shadow: 0 2px 20px rgba(13,45,94,0.07) !important;
    border: 1px solid rgba(26,86,168,0.09) !important;
    max-width: 1400px !important;
}

/* ══════════════════════════════════════════════════════
   品牌 Header — 深蓝渐变 + 扫光 + tech bracket
══════════════════════════════════════════════════════ */
.brand-header {
    background: linear-gradient(120deg,#010c1f 0%,#021630 18%,#0b2a58 45%,#0e3570 60%,#0b2a58 80%,#010c1f 100%);
    border-radius: 0 0 14px 14px;
    padding: 1.5rem 2rem 1.4rem;
    margin: 0 -2rem 2rem;
    display: flex; align-items: center; gap: 1.6rem;
    position: relative; overflow: hidden;
    border-bottom: 2px solid rgba(59,125,216,0.55);
    box-shadow: 0 8px 40px rgba(2,14,40,0.6), inset 0 1px 0 rgba(120,180,255,0.12);
}
.brand-header::before {
    content:''; position:absolute; top:0; left:-80%; width:55%; height:100%;
    background: linear-gradient(90deg,transparent,rgba(120,190,255,0.06),transparent);
    animation: header-sweep 6s ease-in-out infinite; pointer-events:none; z-index:1;
}
@keyframes header-sweep { 0%{left:-80%} 100%{left:130%} }
.brand-header::after {
    content:''; position:absolute; top:-60%; right:-10%; width:55%; height:220%;
    background: radial-gradient(ellipse 60% 60% at 70% 50%,rgba(59,125,216,0.20) 0%,transparent 65%),
                radial-gradient(ellipse 30% 40% at 85% 40%,rgba(100,180,255,0.10) 0%,transparent 60%);
    pointer-events:none; z-index:0;
}
.tc-tl {
    position:absolute; top:10px; left:12px; width:16px; height:16px;
    border-top:2px solid rgba(80,160,255,0.6); border-left:2px solid rgba(80,160,255,0.6);
    border-radius:3px 0 0 0; z-index:3;
}
.tc-br {
    position:absolute; bottom:10px; right:14px; width:16px; height:16px;
    border-bottom:2px solid rgba(80,160,255,0.6); border-right:2px solid rgba(80,160,255,0.6);
    border-radius:0 0 3px 0; z-index:3;
}
.brand-logo {
    width:52px; height:52px; flex-shrink:0;
    background:rgba(255,255,255,0.08); border:1.5px solid rgba(255,255,255,0.22);
    border-radius:10px; display:flex; align-items:center; justify-content:center;
    font-size:1.8rem; position:relative; z-index:2;
}
.brand-text { flex:1; position:relative; z-index:2; }
.brand-title {
    font-size:1.75rem; font-weight:900; letter-spacing:3px; line-height:1.15;
    background:linear-gradient(90deg,#fff 0%,#cce4ff 45%,#fff 70%,#a8d0ff 100%);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text;
    animation:title-glow 4s ease-in-out infinite;
}
@keyframes title-glow {
    0%,100%{filter:drop-shadow(0 0 8px rgba(80,150,255,0.6))}
    50%{filter:drop-shadow(0 0 16px rgba(120,190,255,0.95))}
}
.brand-sub {
    font-size:0.68rem; color:rgba(140,195,255,0.65);
    letter-spacing:2.5px; margin-top:6px; font-weight:600; text-transform:uppercase;
}
.brand-badge {
    font-size:0.70rem; font-weight:700; color:rgba(180,220,255,0.95);
    background:linear-gradient(135deg,rgba(20,60,140,0.55),rgba(10,40,100,0.45));
    border:1px solid rgba(80,150,255,0.5); border-radius:22px;
    padding:6px 18px; position:relative; z-index:2; letter-spacing:1px;
    box-shadow:0 0 16px rgba(59,125,216,0.4);
    animation:pulse-badge 3s ease-in-out infinite;
}
@keyframes pulse-badge {
    0%,100%{box-shadow:0 0 14px rgba(59,125,216,0.35)}
    50%{box-shadow:0 0 26px rgba(80,160,255,0.65)}
}

/* ══════════════════════════════════════════════════════
   隐藏侧边栏（导航已移至顶部）
══════════════════════════════════════════════════════ */
[data-testid="stSidebar"],
[data-testid="collapsedControl"] { display:none !important; }
.block-container { margin-left:0 !important; padding-left:2rem !important; }

/* ══════════════════════════════════════════════════════
   顶部 Tabs 导航 — 大号醒目
══════════════════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
    gap:6px;
    background:linear-gradient(90deg,rgba(13,45,94,0.06),rgba(26,86,168,0.08));
    border-radius:12px; padding:4px 6px;
    border:1px solid rgba(26,86,168,0.14);
    margin-bottom:16px;
}
.stTabs [data-baseweb="tab"] {
    border-radius:9px; font-weight:700; font-size:0.95rem;
    color:#3d5f8f; padding:0.55rem 1.4rem; letter-spacing:0.3px;
    transition:all 0.18s ease;
}
.stTabs [data-baseweb="tab"]:hover {
    background:rgba(26,86,168,0.10) !important; color:#0d2d5e !important;
}
.stTabs [aria-selected="true"] {
    background:linear-gradient(135deg,#1a56a8,#0d2d5e) !important;
    color:#fff !important;
    box-shadow:0 3px 10px rgba(13,45,94,0.28) !important;
}

/* ── 地点列表头 ── */
.loc-list-header {
    background:linear-gradient(90deg,#0d2d5e,#1a56a8);
    color:#fff; padding:8px 14px; border-radius:9px;
    font-weight:700; font-size:13px; margin-bottom:10px;
    letter-spacing:0.5px; box-shadow:0 2px 8px rgba(13,45,94,0.20);
}

/* ══════════════════════════════════════════════════════
   KPI 卡片 — 完全对齐截图四色渐变（蓝/靛/青/紫）
══════════════════════════════════════════════════════ */
.kpi-card {
    border-radius:14px; padding:22px 16px 14px;
    text-align:center; color:#fff; min-height:128px;
    display:flex; flex-direction:column; align-items:center; justify-content:center;
    position:relative; overflow:hidden; transition:transform 0.2s ease;
}
.kpi-card:hover { transform:translateY(-3px); }
.kpi-card::after {
    content:''; position:absolute; right:-18px; bottom:-18px;
    width:80px; height:80px; border-radius:50%; background:rgba(255,255,255,0.08);
}
.kpi-card::before {
    content:''; position:absolute; right:18px; bottom:8px;
    width:46px; height:46px; border-radius:50%; background:rgba(255,255,255,0.06);
}
.kpi-icon { font-size:1.9rem; margin-bottom:6px; position:relative; z-index:1; }
.kpi-num  { font-size:3.1rem; font-weight:800; line-height:1.05;
            letter-spacing:-1px; margin:2px 0; position:relative; z-index:1; }
.kpi-lbl  { font-size:0.88rem; font-weight:600; letter-spacing:0.6px;
            opacity:0.88; margin-top:4px; position:relative; z-index:1; }
.kpi-blue   { background:linear-gradient(135deg,#1a56a8 0%,#0a1f4e 100%);
              box-shadow:0 6px 24px rgba(10,31,78,0.45); }
.kpi-indigo { background:linear-gradient(135deg,#1e66c8 0%,#0d3d8a 100%);
              box-shadow:0 6px 24px rgba(13,61,138,0.42); }
.kpi-teal   { background:linear-gradient(135deg,#0891b2 0%,#0e4f80 100%);
              box-shadow:0 6px 24px rgba(8,80,128,0.40); }
.kpi-purple { background:linear-gradient(135deg,#2563eb 0%,#1e3a8a 100%);
              box-shadow:0 6px 24px rgba(30,58,138,0.42); }

/* 卡片下方展开条（对齐截图） */
.kpi-expand-bar {
    background:#fff; border:1px solid rgba(26,86,168,0.14);
    border-top:none; border-radius:0 0 10px 10px;
    text-align:center; margin-top:-2px; margin-bottom:12px;
}
.kpi-expand { font-size:0.78rem; color:#1a56a8; font-weight:600; padding:7px 0; cursor:pointer; }
.kpi-note   { font-size:0.76rem; color:#888; padding:7px 0; }

/* ── 地点 Header ── */
.loc-header {
    background:linear-gradient(120deg,#010c1f 0%,#0b2a58 40%,#0e3570 60%,#0b2a58 100%);
    color:#fff; border-radius:14px; padding:18px 24px;
    margin-bottom:20px; display:flex; align-items:center;
    gap:16px; flex-wrap:wrap;
    border:1px solid rgba(80,150,255,0.2);
    box-shadow:0 6px 28px rgba(2,14,40,0.30), 0 1px 0 rgba(59,125,216,0.38);
    position:relative; overflow:hidden;
}
.loc-header::before {
    content:''; position:absolute; top:0; right:0; width:40%; height:100%;
    background:radial-gradient(ellipse at 80% 50%,rgba(59,125,216,0.18),transparent 70%);
    pointer-events:none;
}
.loc-city {
    font-size:20px; font-weight:800; letter-spacing:1px;
    background:linear-gradient(90deg,#fff,#cce4ff);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text;
}
.loc-name { font-size:15px; opacity:.82; font-weight:500; }
.loc-stat  { font-size:13px; opacity:.72; margin-left:auto; letter-spacing:0.3px; }

/* ── 分类标签栏 ── */
.sec-bar {
    background:linear-gradient(90deg,#0d2d5e,#1a56a8,#3b7dd8);
    color:#fff; padding:6px 16px; border-radius:8px;
    font-weight:700; font-size:13px; margin:12px 0 8px;
    letter-spacing:0.5px; box-shadow:0 2px 8px rgba(13,45,94,0.20);
}

/* ── 提示条 ── */
.tip {
    background:linear-gradient(135deg,rgba(238,242,249,0.98),rgba(187,222,251,0.38));
    border-left:4px solid #1a56a8; border-radius:8px;
    padding:10px 16px; font-size:13px; color:#2d4a6f;
    margin-bottom:14px; border:1px solid rgba(26,86,168,0.12);
}

/* ── 标题 ── */
h2,h3 { color:#0d2d5e !important; }
h2 { padding-bottom:6px; border-bottom:2px solid rgba(26,86,168,0.18); margin-bottom:16px; }
h3 { margin-top:22px; }
hr { border-color:rgba(26,86,168,0.14) !important; }

/* ── 主要按钮 ── */
.stButton>button[kind="primary"] {
    background:linear-gradient(90deg,#0d2d5e,#1a56a8) !important;
    border:none !important; border-radius:8px !important;
    font-weight:700 !important; letter-spacing:0.5px !important;
    box-shadow:0 3px 12px rgba(13,45,94,0.28) !important;
    transition:all 0.18s ease !important;
}
.stButton>button[kind="primary"]:hover {
    background:linear-gradient(90deg,#1a56a8,#3b7dd8) !important;
    box-shadow:0 6px 20px rgba(26,86,168,0.38) !important;
    transform:translateY(-1px) !important;
}

/* ── 通用按钮 ── */
[data-testid="stMain"] .stButton>button {
    font-size:0.85rem !important; font-weight:500 !important;
    min-height:34px !important; padding:0.3rem 0.8rem !important;
    color:#1a56a8 !important; background:rgba(26,86,168,0.07) !important;
    border:1px solid rgba(26,86,168,0.26) !important; border-radius:8px !important;
    transition:all 0.17s ease !important;
}
[data-testid="stMain"] .stButton>button:hover {
    background:rgba(26,86,168,0.13) !important;
    border-color:rgba(26,86,168,0.48) !important; transform:translateY(-1px) !important;
}

/* ── 进度条 ── */
[data-testid="stProgressBar"]>div { background:linear-gradient(90deg,#1a56a8,#3b7dd8) !important; }

/* ── Multiselect 标签 ── */
[data-testid="stMultiSelect"] [data-baseweb="tag"] {
    background:rgba(26,86,168,0.10) !important; border:1px solid rgba(26,86,168,0.28) !important;
    border-radius:6px !important; font-size:0.75rem !important; color:#0d2d5e !important;
}
[data-testid="stMultiSelect"] label { font-size:0.80rem !important; font-weight:600 !important; color:#3d5f8f !important; }

/* ── 表单 ── */
[data-testid="stForm"] {
    border:1px solid rgba(26,86,168,0.16); border-radius:14px;
    padding:18px; background:rgba(250,252,255,0.97);
    box-shadow:0 2px 10px rgba(13,45,94,0.04);
}

/* ── Border 容器 ── */
[data-testid="stVerticalBlockBorderWrapper"] {
    background:#fff !important; border:1px solid rgba(26,86,168,0.14) !important; border-radius:13px !important;
}

/* ── Expander ── */
details summary {
    background:rgba(230,240,255,0.72) !important; border-radius:9px !important; padding:0.5rem 1rem !important;
}


/* ── Dataframe ── */
[data-testid="stDataFrame"] { border-radius:9px !important; overflow:hidden !important; border:1px solid rgba(26,86,168,0.13) !important; }

/* ── 图片 meta ── */
.img-card-meta { padding:6px 10px 8px; font-size:0.72rem; color:#2a4a7f; line-height:1.55; }
.img-card-meta b { color:#0d2d5e; }

/* ── Metric ── */
[data-testid="stMetric"] {
    background:linear-gradient(135deg,rgba(238,242,249,0.92),rgba(187,222,251,0.55));
    border-radius:12px; padding:16px 20px;
    border:1px solid rgba(26,86,168,0.14); box-shadow:0 2px 10px rgba(13,45,94,0.07);
}
[data-testid="stMetricLabel"] p { color:#1a56a8 !important; font-weight:600; }
[data-testid="stMetricValue"]   { color:#0d2d5e !important; }

/* ── Caption ── */
.stCaption { color:#3d5f8f !important; }
</style>
"""

# ─── 主入口 ──────────────────────────────────────────────────────────
def main():
    st.set_page_config(
        page_title="驭研科技大规模自然驾驶数据集统计平台",
        page_icon="🛰️",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.markdown(CSS, unsafe_allow_html=True)
    init_db()

    # Brand Header（与截图完全一致）
    st.markdown("""
<div class="brand-header">
  <div class="tc-tl"></div><div class="tc-br"></div>
  <div class="brand-logo">🛰️</div>
  <div class="brand-text">
    <div class="brand-title">驭研科技大规模自然驾驶数据集统计平台</div>
    <div class="brand-sub">DRIVEResearch · Operational Design Domain Labeling Platform</div>
  </div>
  <div class="brand-badge">🗺️ Aerial · ODD Labeling</div>
</div>
""", unsafe_allow_html=True)

    tab1 = st.tabs(["📊 统计看板"])
    with tab1:
        page_dashboard()


if __name__ == "__main__":
    main()