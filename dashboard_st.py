"""
标注数据可视化看板 - 展示 DB 中各标签类别的统计与数据多样性
使用缓存、预解析、多线程加速统计

【数据来源】
  SQLite 数据库 labeling_data_v2.db，核心表 dataset，字段：
    id, video_name, folder_path, location_name, collection_time,
    top_road_category, top_road_subcategory, secondary_tags_json, duration

【secondary_tags_json 结构示例】
  {
    "一、道路静态环境": {
      "1.2 道路表面": { "表面类型": "沥青", "表面状态": ["干燥", "潮湿"] },
      "1.6 道路交叉": { "交叉类型": ["T型交叉", "十字交叉"] }
    },
    "四、大气环境": {
      "4.1 天气": { "类型": "晴天" }
    }
  }
  标签值可以是字符串（单选）或列表（多选），flatten 函数负责统一展开为列表。
"""
import streamlit as st
import pandas as pd
import sqlite3
import json
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_FILE = "labeling_data_v2.db"

# 地点名称映射：folder_path 中的英文 -> 中文（如 2024-ShenZhen-AerialVideo-V1/ADS_WZY_22 -> 深圳）
CITY_MAP = {
    "ShenZhen": "深圳",
    "Changchun": "长春",
    "Hongkong": "香港",
    "Beijing": "北京",
    "Shanghai": "上海",
    "Guangzhou": "广州",
    "Hangzhou": "杭州",
    "Wuhan": "武汉",
    "Chengdu": "成都",
}


def extract_city_from_path(folder_path: str) -> str:
    """从 folder_path 提取城市。有视频: /.../2024-ShenZhen-AerialVideo-V1/ADS_xx -> 深圳；无视频: MANUAL/深圳/2024/ADS_1 -> 深圳
    输入: folder_path 字符串，如 "2024-ShenZhen-AerialVideo-V1/ADS_WZY_22"
    输出: 中文城市名，如 "深圳"；无法识别时返回 "未知"
    """
    if not folder_path:
        return "未知"
    if folder_path.startswith("MANUAL/"):
        parts = folder_path.split("/")
        if len(parts) >= 2:
            return parts[1] or "未知"
    m = re.search(r"\d{4}-([A-Za-z]+)-AerialVideo", folder_path)
    if m:
        en = m.group(1)
        return CITY_MAP.get(en, en)
    return "未知"


def parse_hour_from_collection_time(collection_time_str) -> int:
    """从 collection_time 解析小时，格式如 2025年8月18日 11:51 -> 11
    输入: collection_time 字符串，如 "2025年8月18日 11:51"
    输出: 小时整数 (0-23)；无法解析时返回 -1
    """
    if pd.isna(collection_time_str) or not collection_time_str:
        return -1
    m = re.search(r"\s(\d{1,2}):(\d{2})", str(collection_time_str))
    if m:
        return int(m.group(1))
    return -1


def categorize_time_period(hour: int) -> str:
    """按小时分类：早高峰 07:00-09:00, 晚高峰 17:00-19:00, 夜间 19:00以后, 日常规 其余（含未知）
    输入: 小时整数 (0-23)，-1 表示未知
    输出: 时段名称字符串，如 "早高峰 (07:00-09:00)"
    """
    if hour < 0:
        return "日常规"  # 无法解析的按日常规统计
    if 7 <= hour < 9:
        return "早高峰 (07:00-09:00)"
    if 17 <= hour < 19:
        return "晚高峰 (17:00-19:00)"
    if hour >= 19:
        return "夜间 (19:00以后)"
    return "日常规"


def extract_year_from_path(folder_path: str) -> str:
    """从 folder_path 提取年份。有视频: 2024-ShenZhen...；无视频: MANUAL/深圳/2024/ADS_1 -> 2024
    输入: folder_path 字符串
    输出: 年份字符串，如 "2024"；无法识别时返回 ""
    """
    if not folder_path:
        return ""
    if folder_path.startswith("MANUAL/"):
        parts = folder_path.split("/")
        if len(parts) >= 3:
            return parts[2] or ""
    m = re.search(r"(\d{4})-", folder_path)
    return m.group(1) if m else ""


@st.cache_data(ttl=300)
def load_data(db_path: str = DB_FILE) -> pd.DataFrame:
    """加载 dataset 表并解析字段，缓存 5 分钟
    输入: db_path - SQLite 数据库文件路径
    输出: DataFrame，列包括：
        原始列: id, video_name, folder_path, location_name, collection_time,
                top_road_category, top_road_subcategory, secondary_tags_json, duration（分钟）
        衍生列: city（从 folder_path 解析的中文城市名）
                year（从 folder_path 解析的年份字符串）
                _parsed_json（secondary_tags_json 预解析后的 dict，避免后续重复 JSON 解析）
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        "SELECT id, video_name, folder_path, location_name, collection_time, "
        "top_road_category, top_road_subcategory, secondary_tags_json, duration "
        "FROM dataset",
        conn,
    )
    conn.close()
    df["city"] = df["folder_path"].apply(extract_city_from_path)
    df["year"] = df["folder_path"].apply(extract_year_from_path)
    # 预解析 JSON，避免后续重复解析
    def _parse_sec(s):
        if pd.isna(s) or not s:
            return {}
        try:
            return json.loads(s)
        except Exception:
            return {}
    df["_parsed_json"] = df["secondary_tags_json"].apply(_parse_sec)
    return df


def _extract_tag_from_parsed(d: dict, section: str, sub: str, attr: str):
    """从预解析的 dict 提取标签值
    输入:
        d      - 预解析的 secondary_tags_json dict
        section - 一级节，如 "一、道路静态环境"
        sub    - 二级节，如 "1.2 道路表面"
        attr   - 属性名，如 "表面类型"
    输出: 标签值列表（统一为 list），如 ["沥青"] 或 ["T型", "十字"]；无值时返回 None
    """
    if not d:
        return None
    s = d.get(section, {})
    sub_d = s.get(sub, {})
    val = sub_d.get(attr)
    if val is None:
        return None
    if isinstance(val, list):
        return val if val else None
    return [val] if val else None


def flatten_tags_for_count(df: pd.DataFrame, section: str, sub: str, attr: str) -> pd.Series:
    """将多选标签展开为单行计数，使用预解析的 _parsed_json
    输入:
        df      - 包含 _parsed_json（或 secondary_tags_json）列的 DataFrame
        section, sub, attr - 三级标签路径
    输出: pd.Series，每个元素是一个标签值字符串（多选已展开，一个值一行）
          例如一条记录有 ["沥青", "混凝土"]，则贡献两行
          可直接用 .value_counts() 统计各值出现次数
    """
    if "_parsed_json" in df.columns:
        vals = df["_parsed_json"].apply(lambda d: _extract_tag_from_parsed(d, section, sub, attr))
    else:
        def _safe_parse(x):
            try:
                return json.loads(x) if pd.notna(x) and x else {}
            except Exception:
                return {}
        vals = df["secondary_tags_json"].apply(
            lambda x: _extract_tag_from_parsed(_safe_parse(x), section, sub, attr)
        )
    all_items = []
    for v in vals:
        if v:
            all_items.extend(v)
    return pd.Series(all_items)


def flatten_tags_for_duration(df: pd.DataFrame, section: str, sub: str, attr: str) -> pd.DataFrame:
    """将多选标签展开，每个标签值关联 duration，使用预解析与 zip 加速
    输入:
        df      - 包含 _parsed_json 和 duration（分钟）列的 DataFrame
        section, sub, attr - 三级标签路径
    输出: DataFrame，列为：
        tag_value  - 标签值字符串（多选已展开）
        duration   - 对应记录的视频时长（分钟，float）
        若无有效数据则返回空 DataFrame（含上述两列）
    """
    if "_parsed_json" not in df.columns:
        df = df.copy()
        df["_parsed_json"] = df["secondary_tags_json"].apply(
            lambda x: json.loads(x) if pd.notna(x) and x else {}
        )
    durations = df["duration"].fillna(0)
    rows = []
    for parsed, dur in zip(df["_parsed_json"], durations):
        vals = _extract_tag_from_parsed(parsed, section, sub, attr)
        d = float(dur) if dur and dur > 0 else 0
        if vals:
            for v in vals:
                rows.append({"tag_value": v, "duration": d})
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["tag_value", "duration"])


def run():
    st.set_page_config(page_title="标注数据可视化", layout="wide")
    st.title("📊 标注数据统计看板")
    st.caption("基于 labeling_data_v2.db 的标签多样性分析")

    # df: 完整原始数据（含衍生列 city/year/_parsed_json），供全局使用
    df = load_data()
    if df.empty:
        st.warning("数据库为空")
        return

    # ========== 侧边栏 ==========
    st.sidebar.header("🔍 筛选条件")
    st.sidebar.caption("💡 地点从 folder_path 解析，如 2024-ShenZhen-AerialVideo-V1 → 深圳")
    if st.sidebar.button("🔄 清除缓存", help="数据更新后点击以重新加载"):
        load_data.clear()
        st.rerun()
    cities = sorted(df["city"].dropna().unique())
    selected_cities = st.sidebar.multiselect("城市", cities, default=cities)
    years = sorted(df["year"].dropna().unique())
    selected_years = st.sidebar.multiselect("年份", years, default=years)
    road_cats = sorted(df["top_road_category"].dropna().unique())
    selected_road = st.sidebar.multiselect("道路类型", road_cats, default=road_cats)

    # mask: 布尔索引，根据三个筛选条件过滤行
    # df_f: 筛选后的 DataFrame，后续所有统计均基于此
    mask = (
        df["city"].isin(selected_cities)
        & df["year"].isin(selected_years)
        & df["top_road_category"].isin(selected_road)
    )
    df_f = df[mask]

    # 概览指标
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("视频总数", len(df_f))
    with col2:
        st.metric("覆盖城市", df_f["city"].nunique())
    with col3:
        total_dur = df_f["duration"].sum()
        hrs = total_dur / 60 if pd.notna(total_dur) else 0
        st.metric("总时长 (小时)", f"{hrs:.1f}")
    with col4:
        st.metric("地点数", df_f["folder_path"].nunique())

    st.divider()

    # ========== 1. 城市与年份分布 ==========
    st.subheader("📍 地域与时间分布")
    # df_dur: 在 df_f 基础上补充 duration_hours 列（分钟/60），供时长类图表使用
    df_dur = df_f.copy()
    df_dur["duration"] = df_dur["duration"].fillna(0)
    df_dur["duration_hours"] = df_dur["duration"] / 60  # 统一用小时展示
    c1, c2 = st.columns(2)
    with c1:
        # city_dur: 按城市聚合总时长，排除时长为0的城市
        city_dur = df_dur.groupby("city")["duration_hours"].sum().reset_index(name="duration_hours")
        city_dur = city_dur[city_dur["duration_hours"] > 0]
        if not city_dur.empty:
            try:
                import plotly.express as px
                fig = px.bar(
                    city_dur,
                    x="city",
                    y="duration_hours",
                    labels={"city": "城市", "duration_hours": "总时长(小时)"},
                    title="各城市视频总时长",
                )
                fig.update_traces(texttemplate="%{y:.1f}", textposition="outside", textfont_size=10)
                _ymax = city_dur["duration_hours"].max()
                fig.update_layout(
                    showlegend=False, margin=dict(t=80), yaxis=dict(range=[0, _ymax * 1.15]),
                )
                st.plotly_chart(fig, width='content')
            except ImportError:
                st.bar_chart(city_dur.set_index("city")["duration_hours"])
        else:
            st.info("暂无时长数据")

    with c2:
        # year_city: 按年份+城市聚合总时长，用于堆叠柱状图
        year_city = df_dur.groupby(["year", "city"])["duration_hours"].sum().reset_index(name="duration_hours")
        year_city = year_city[year_city["duration_hours"] > 0]
        if not year_city.empty:
            try:
                import plotly.express as px
                fig = px.bar(
                    year_city,
                    x="year",
                    y="duration_hours",
                    color="city",
                    barmode="stack",
                    labels={"duration_hours": "总时长(小时)"},
                    title="年份 × 城市 总时长分布",
                )
                fig.update_traces(texttemplate="%{y:.1f}", textposition="inside", textfont_size=10)
                st.plotly_chart(fig, width='content')
            except ImportError:
                st.bar_chart(year_city.set_index("year")["duration_hours"])
        else:
            st.info("暂无时长数据")

    # ========== 1.5 采集时段分布（早高峰/晚高峰/日常规） ==========
    st.subheader("🕐 采集时段分布")
    # _coll_hour: collection_time 解析出的小时（int），-1 表示无法解析
    # time_period: 根据小时归入 4 个时段之一
    df_dur["_coll_hour"] = df_dur["collection_time"].apply(parse_hour_from_collection_time)
    df_dur["time_period"] = df_dur["_coll_hour"].apply(categorize_time_period)
    period_order = ["早高峰 (07:00-09:00)", "日常规", "晚高峰 (17:00-19:00)", "夜间 (19:00以后)"]
    # period_df: 各时段总时长，只保留有有效时长（>0）的行，并按预定顺序排序
    period_df = df_dur[df_dur["duration_hours"] > 0].groupby("time_period")["duration_hours"].sum().reset_index(name="duration_hours")
    period_df["_ord"] = period_df["time_period"].apply(lambda x: period_order.index(x) if x in period_order else 99)
    period_df = period_df.sort_values("_ord").drop(columns=["_ord"])
    if not period_df.empty:
        try:
            import plotly.express as px
            fig = px.bar(
                period_df,
                x="time_period",
                y="duration_hours",
                labels={"time_period": "时段", "duration_hours": "总时长(小时)"},
                title="采集时段分布 (早高峰 07:00-09:00 | 晚高峰 17:00-19:00 | 夜间 19:00以后 | 日常规 其余)",
            )
            fig.update_traces(texttemplate="%{y:.1f}", textposition="outside", textfont_size=11)
            _ymax = period_df["duration_hours"].max()
            fig.update_layout(
                xaxis_title="时段", yaxis_title="总时长(小时)",
                margin=dict(t=80), yaxis=dict(range=[0, _ymax * 1.15]),
            )
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            st.bar_chart(period_df.set_index("time_period")["duration_hours"])
    else:
        st.info("暂无采集时间数据")

    # ========== 2. 道路类型（含 1.6 道路交叉-交叉类型） ==========
    st.subheader("🛣️ 道路类型")
    top_df = df_dur[["top_road_category", "top_road_subcategory", "_parsed_json", "duration_hours"]].dropna(subset=["top_road_category"])
    top_df = top_df[top_df["top_road_category"].str.len() > 0]
    if not top_df.empty:
        # 展开 交叉类型（多选），合并到道路类型层级
        # rows 最终结构: [{top_road_category, top_road_subcategory, 交叉类型, duration_hours}, ...]
        rows = []
        for _, row in top_df.iterrows():
            vals = _extract_tag_from_parsed(row["_parsed_json"], "一、道路静态环境", "1.6 道路交叉", "交叉类型")
            cross_types = vals if vals else ["未标注"]
            dur = row["duration_hours"] or 0
            if dur <= 0:
                continue
            for ct in cross_types:
                rows.append({
                    "top_road_category": row["top_road_category"],
                    "top_road_subcategory": row["top_road_subcategory"],
                    "交叉类型": ct,
                    "duration_hours": dur,
                })
        if rows:
            # 三级旭日图：道路主类 → 道路子类 → 交叉类型
            top_agg = pd.DataFrame(rows).groupby(["top_road_category", "top_road_subcategory", "交叉类型"])["duration_hours"].sum().reset_index(name="duration_hours")
            top_agg = top_agg[top_agg["duration_hours"] > 0]
            if not top_agg.empty:
                try:
                    import plotly.express as px
                    fig = px.sunburst(
                        top_agg,
                        path=["top_road_category", "top_road_subcategory", "交叉类型"],
                        values="duration_hours",
                        title="道路类型层级分布（扇区大小=总时长/小时）",
                    )
                    st.plotly_chart(fig, width='content')
                except ImportError:
                    st.bar_chart(top_agg.groupby("top_road_category")["duration_hours"].sum())
            else:
                st.info("暂无时长数据")
        else:
            # 无交叉类型数据时退化为二级旭日图：道路主类 → 道路子类
            top_agg = top_df.groupby(["top_road_category", "top_road_subcategory"])["duration_hours"].sum().reset_index(name="duration_hours")
            top_agg = top_agg[top_agg["duration_hours"] > 0]
            if not top_agg.empty:
                try:
                    import plotly.express as px
                    fig = px.sunburst(
                        top_agg,
                        path=["top_road_category", "top_road_subcategory"],
                        values="duration_hours",
                        title="道路类型层级分布（扇区大小=总时长/小时）",
                    )
                    st.plotly_chart(fig, width='content')
                except ImportError:
                    st.bar_chart(top_agg.groupby("top_road_category")["duration_hours"].sum())
            else:
                st.info("暂无时长数据")
    else:
        st.info("暂无道路类型数据")

    # TAG_PATHS: 所有需要统计的三级标签路径，格式 (section, sub, attr)
    # section → secondary_tags_json 的一级 key（如 "一、道路静态环境"）
    # sub     → 二级 key（如 "1.2 道路表面"）
    # attr    → 具体属性名（如 "表面类型"）
    TAG_PATHS = [
        ("一、道路静态环境", "1.2 道路表面", "表面类型"),
        ("一、道路静态环境", "1.2 道路表面", "表面状态"),
        ("一、道路静态环境", "1.3 道路几何", "坡度"),
        ("一、道路静态环境", "1.3 道路几何", "曲率"),
        ("一、道路静态环境", "1.4 包含车道特征", "最宽车道数量"),
        ("一、道路静态环境", "1.4 包含车道特征", "车道类型"),
        ("一、道路静态环境", "1.5 道路边缘", "边缘类型"),
        ("一、道路静态环境", "1.6 道路交叉", "交叉类型"),
        ("二、交通设施", "2.1 交通控制", "信号灯"),
        ("二、交通设施", "2.1 交通控制", "地面标签"),
        ("二、交通设施", "2.2 路侧与周边环境", "设施"),
        ("二、交通设施", "2.3 特殊设施", "类型"),
        ("三、动态目标 (路面状况)", "3.1 机动车", "类型"),
        ("三、动态目标 (路面状况)", "3.2 VRU", "类型"),
        ("三、动态目标 (路面状况)", "3.3 动物", "类型"),
        ("三、动态目标 (路面状况)", "3.4 障碍物", "类型"),
        ("三、动态目标 (路面状况)", "3.5 事故车辆", "类型"),
        ("四、大气环境", "4.1 天气", "类型"),
        ("四、大气环境", "4.2 颗粒物", "类型"),
        ("四、大气环境", "4.3 光照", "来源"),
        ("四、大气环境", "4.3 光照", "强度"),
        ("四、大气环境", "4.4 气温", "估算"),
    ]

    # ========== 2.5 数据多样性概览 ==========
    def _compute_one_tag_stats(args):
        """多线程任务单元：计算单个标签路径的统计指标
        输入: args = (section, sub, attr) 三元组
        输出: dict，包含：
            标签       - "sub - attr" 格式的标签名
            唯一值数   - 该标签出现过多少种不同值
            标注次数   - 展开后的总标注行数（多选按条数计）
            总时长(小时) - 有该标签的视频总时长（小时）
        """
        s, sub, attr = args
        cnts = flatten_tags_for_count(df_f, s, sub, attr)
        dur_df = flatten_tags_for_duration(df_dur, s, sub, attr)
        total_hrs = dur_df["duration"].sum() / 60 if not dur_df.empty and "duration" in dur_df.columns else 0
        return {
            "标签": f"{sub} - {attr}",
            "唯一值数": cnts.nunique() if not cnts.empty else 0,
            "标注次数": len(cnts),
            "总时长(小时)": round(total_hrs, 1),
        }

    with st.expander("📋 各标签唯一值数量（数据多样性）", expanded=False):
        # 用 ThreadPoolExecutor 并发计算所有标签的多样性指标，最多 8 线程
        with ThreadPoolExecutor(max_workers=8) as ex:
            diversity = list(ex.map(_compute_one_tag_stats, TAG_PATHS))
        div_df = pd.DataFrame(diversity)
        st.dataframe(div_df, width='content')

    # ========== 3. 次级标签统计（多选展开） ==========
    st.subheader("🏷️ 标签分布 - 数据多样性")
    # 标签选择
    tag_options = [f"{sub} - {attr}" for s, sub, attr in TAG_PATHS]
    selected_tag = st.selectbox("选择要查看的标签", tag_options, index=0)
    idx = tag_options.index(selected_tag)
    section, sub, attr = TAG_PATHS[idx]

    # tag_dur_df: 选中标签展开后的 DataFrame，列为 [tag_value, duration（分钟）]
    tag_dur_df = flatten_tags_for_duration(df_dur, section, sub, attr)
    if not tag_dur_df.empty and tag_dur_df["duration"].sum() > 0:
        # agg_df: 按标签值聚合总时长，列为 [tag_value, duration（分钟）, 总时长(小时)]
        agg_df = tag_dur_df.groupby("tag_value")["duration"].sum().reset_index(name="duration")
        agg_df["总时长(小时)"] = agg_df["duration"] / 60
        chart_type = st.radio("图表类型", ["柱状图", "饼图"], horizontal=True, key="chart_type")
        try:
            import plotly.express as px
            if chart_type == "饼图":
                fig = px.pie(
                    agg_df,
                    values="总时长(小时)",
                    names="tag_value",
                    title=f"{sub} - {attr}",
                )
                fig.update_traces(
                    texttemplate="%{label}<br>%{percent:.1%} · %{value:.1f}h",
                    textposition="outside",
                )
            else:
                fig = px.bar(
                    agg_df,
                    x="tag_value",
                    y="总时长(小时)",
                    labels={"tag_value": "标签值"},
                    title=f"{sub} - {attr}",
                )
                fig.update_traces(texttemplate="%{y:.1f}", textposition="outside", textfont_size=11)
                fig.update_xaxes(tickangle=-45)
                _ymax = agg_df["总时长(小时)"].max()
                fig.update_layout(margin=dict(t=80), yaxis=dict(range=[0, _ymax * 1.15]))
            st.plotly_chart(fig, width='content')
        except ImportError:
            st.bar_chart(agg_df.set_index("tag_value")["总时长(小时)"])
    else:
        st.info("该标签无有效数据")

    # ========== 4. 多维度交叉统计 ==========
    st.subheader("📈 多维度交叉统计")
    # DIM_OPTIONS: 可选的交叉统计维度
    # 元组格式 (dim_key, 显示名)
    #   dim_key 若等于 df_dur 中已有列名（city/top_road_category/top_road_subcategory），直接使用
    #   否则格式为 "sub-attr"，_add_dim_col 会从 secondary_tags_json 解析并追加为新列
    DIM_OPTIONS = [
        ("city", "城市"),
        ("top_road_category", "道路主类"),
        ("top_road_subcategory", "道路子类"),
        ("1.6 道路交叉-交叉类型", "交叉类型"),
        ("1.2 道路表面-表面类型", "表面类型"),
        ("1.2 道路表面-表面状态", "表面状态"),
        ("2.1 交通控制-信号灯", "信号灯"),
        ("4.1 天气-类型", "天气"),
        ("4.3 光照-强度", "光照强度"),
    ]
    dim_labels = [f"{v} ({k})" for k, v in DIM_OPTIONS]
    dim_keys = [k for k, v in DIM_OPTIONS]

    def _add_dim_col(df, dim_key):
        """懒加载：若 df 中尚无 dim_key 列，则从 secondary_tags_json 解析后追加
        输入:
            df      - 含 _parsed_json 列的 DataFrame
            dim_key - 维度 key，如 "city"（已有列）或 "1.6 道路交叉-交叉类型"（需解析）
        输出: 含 dim_key 列的 DataFrame（多选只取第一个值，无值标注为 "未标注"）
        """
        if dim_key in df.columns:
            return df
        if dim_key in ("city", "top_road_category", "top_road_subcategory"):
            return df
        # 解析 secondary tag: "1.6 道路交叉-交叉类型" -> sub, attr
        for s, sub, attr in TAG_PATHS:
            if f"{sub}-{attr}" == dim_key:
                vals = df["_parsed_json"].apply(lambda d: _extract_tag_from_parsed(d, s, sub, attr))
                df = df.copy()
                df[dim_key] = vals.apply(lambda v: v[0] if v and len(v) > 0 else "未标注")
                break
        return df

    sel1 = st.selectbox("维度 1", dim_labels, key="d1")
    sel2 = st.selectbox("维度 2", dim_labels, key="d2")
    idx1, idx2 = dim_labels.index(sel1), dim_labels.index(sel2)
    dim1, dim2 = dim_keys[idx1], dim_keys[idx2]
    if dim1 != dim2:
        df_cross = _add_dim_col(df_dur.copy(), dim1)
        df_cross = _add_dim_col(df_cross, dim2)
        if dim1 in df_cross.columns and dim2 in df_cross.columns:
            # cross: 两维度组合后的总时长，pivot 成热力矩阵展示
            cross = df_cross.groupby([dim1, dim2])["duration_hours"].sum().reset_index(name="总时长(小时)")
            cross = cross[cross["总时长(小时)"] > 0]
            if not cross.empty:
                pivot = cross.pivot(index=dim1, columns=dim2, values="总时长(小时)").fillna(0)
                st.dataframe(pivot.style.background_gradient(cmap="Blues"))
            else:
                st.info("暂无时长数据")
        else:
            st.info("维度数据不可用")

    st.divider()

    # ========== 6. 原始数据预览 ==========
    if st.expander("📋 查看筛选后数据"):
        st.dataframe(
            df_f[
                ["video_name", "folder_path", "city", "year", "top_road_category", "top_road_subcategory"]
            ].head(100),
            width='content',
        )


if __name__ == "__main__":
    run()
