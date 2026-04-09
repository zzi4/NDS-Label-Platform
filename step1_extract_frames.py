"""
Step 1: 从视频目录中提取每个三级文件夹（采集时段）的代表帧
- 遍历所有 L1/L2/L3 结构，找最后一个 MP4，抽取第 0 帧
- 统计该 L3 下所有 MP4 的总时长（分钟）
- 输出图片到 /mnt/nas/Processing_data/Label_cites/ 保持相同目录结构
- 生成索引文件 sessions_index.json 供 step2 使用
- 每个 (L2, 日期) 组生成日代表图：取当天首/中/尾三个 L3 的图片

特殊结构处理：
  - ShenZhen: L3 下还有 L4 子文件夹，递归找 MP4
  - Haerbin: MP4 直接在 L2（无 L3），L2 本身作为唯一 session
  - Hongkong: L2 下有 'video' 子文件夹作为 pseudo-L3

运行：
  conda activate nds && python step1_extract_frames.py           # 断点续跑
  conda activate nds && python step1_extract_frames.py --reset  # 清空重新提取
"""

import os
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import cv2

NUM_WORKERS = os.cpu_count() or 4

VIDEO_ROOT = "/mnt/nas/ADSafety/ADSafety/Aerial_Raw_Videos"
OUTPUT_ROOT = "/mnt/nas/Processing_data/Label_cites"
INDEX_FILE = "/home/stu1/Projects/LabelWork/sessions_index.json"
VIDEO_EXTS = {".mp4", ".MP4"}


# ─────────────────────────── 工具函数 ────────────────────────────

def find_mp4s(folder: Path) -> list:
    """递归找 folder 下所有 MP4 文件，按名称排序"""
    found = []
    for root, _, files in os.walk(folder):
        for f in sorted(files):
            if Path(f).suffix in VIDEO_EXTS:
                found.append(Path(root) / f)
    return sorted(found)


def get_video_duration_minutes(video_path: Path) -> float:
    """用 OpenCV 获取视频时长（分钟）"""
    try:
        cap = cv2.VideoCapture(str(video_path))
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        cap.release()
        if fps > 0 and frame_count > 0:
            return frame_count / fps / 60.0
    except Exception:
        pass
    # 回退：文件大小估算（1GB ≈ 1分钟）
    try:
        return video_path.stat().st_size / (1024 ** 3)
    except Exception:
        return 0.0


def extract_first_frame(video_path: Path, output_jpg: Path) -> bool:
    """用 OpenCV 抽取视频第 0 帧为 JPG"""
    output_jpg.parent.mkdir(parents=True, exist_ok=True)
    if output_jpg.exists():
        return True
    try:
        cap = cv2.VideoCapture(str(video_path))
        ret, frame = cap.read()
        cap.release()
        if ret:
            cv2.imwrite(str(output_jpg), frame)
            return output_jpg.exists()
    except Exception as e:
        print(f"    [ERROR] OpenCV 失败 {video_path.name}: {e}")
    return False


def parse_collection_time(video_name: str) -> str:
    """从文件名解析采集时间，格式：DJI_20250711155546_0001_V → 2025年7月11日 15:55"""
    m = re.search(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})", video_name)
    if m:
        y, mo, d, h, mi = m.groups()
        return f"{y}年{int(mo)}月{int(d)}日 {int(h):02d}:{int(mi):02d}"
    return ""


def parse_date_str(name: str) -> str:
    """从名称中提取日期字符串 YYYYMMDD，用于按天分组"""
    m = re.search(r"(\d{8})", name)
    return m.group(1) if m else ""


# ──────────────────────────── 核心处理 ────────────────────────────

def process_session(l1_name: str, l2_name: str, l3_name: str,
                    session_folder: Path, relative_path: str):
    """
    处理一个 session：找最后一个 MP4、提取其第 0 帧、算总时长
    返回 session 信息字典，失败返回 None
    """
    mp4s = find_mp4s(session_folder)
    if not mp4s:
        print(f"    [SKIP] 无 MP4: {session_folder}")
        return None

    last_mp4 = mp4s[-1]   # 取最后一个视频
    video_name = last_mp4.name

    output_jpg = Path(OUTPUT_ROOT) / relative_path / (last_mp4.stem + ".jpg")

    ok = extract_first_frame(last_mp4, output_jpg)
    if not ok:
        print(f"    [ERROR] 帧提取失败: {relative_path}")
        return None

    total_duration = sum(get_video_duration_minutes(mp4) for mp4 in mp4s)
    collection_time = parse_collection_time(video_name)

    return {
        "l1": l1_name,
        "l2": l2_name,
        "l3": l3_name,
        "folder_path": relative_path,
        "video_name": video_name,
        "location_name": l2_name,
        "collection_time": collection_time,
        "duration": round(total_duration, 4),
        "image_path": str(output_jpg),
        "mp4_count": len(mp4s),
    }


def build_day_summaries(sessions: list) -> list:
    """
    对每个 (l1, l2, 日期) 分组，选出首/中/尾三个 session 作为当天代表图。
    日期从 l3 名称解析，回退到 video_name。
    """
    groups = defaultdict(list)
    for s in sessions:
        date = parse_date_str(s["l3"]) or parse_date_str(s["video_name"])
        groups[(s["l1"], s["l2"], date)].append(s)

    summaries = []
    for (l1, l2, date), group in sorted(groups.items()):
        group = sorted(group, key=lambda s: s["l3"])
        n = len(group)

        # 选首/中/尾，去重
        candidates = [group[0], group[n // 2], group[-1]]
        seen, picks = set(), []
        for p in candidates:
            if p["folder_path"] not in seen:
                seen.add(p["folder_path"])
                picks.append(p)

        date_display = ""
        if len(date) == 8:
            date_display = f"{date[:4]}年{int(date[4:6])}月{int(date[6:8])}日"

        total_duration = round(sum(s["duration"] for s in group), 4)

        representatives = [
            {
                "folder_path": p["folder_path"],
                "image_path": p["image_path"],
                "collection_time": p["collection_time"],
                "duration": p["duration"],
            }
            for p in picks
        ]

        summaries.append({
            "l1": l1,
            "l2": l2,
            "date": date,
            "date_display": date_display,
            "session_count": n,
            "total_duration": total_duration,
            "representatives": representatives,
        })

    return summaries


# ────────────────────────── 目录扫描 ──────────────────────────────

def collect_tasks(video_root: Path, done_paths: set) -> list:
    """
    Phase 1：单线程遍历目录，收集所有待处理任务。
    返回 list of (l1_name, l2_name, l3_name, session_folder, rel)
    """
    tasks = []
    for l1_dir in sorted(video_root.iterdir()):
        if not l1_dir.is_dir():
            continue
        l1_name = l1_dir.name

        for l2_dir in sorted(l1_dir.iterdir()):
            if not l2_dir.is_dir():
                continue
            l2_name = l2_dir.name

            # ── 特殊情况1：Haerbin —— MP4 直接在 L2，无 L3 ──
            mp4s_in_l2 = [f for f in l2_dir.iterdir()
                          if f.is_file() and f.suffix in VIDEO_EXTS]
            if mp4s_in_l2:
                rel = f"{l1_name}/{l2_name}"
                if rel not in done_paths:
                    tasks.append((l1_name, l2_name, l2_name, l2_dir, rel))
                continue

            # ── 特殊情况2：Hongkong —— L2 下有 'video' 子文件夹 ──
            video_subdir = l2_dir / "video"
            if video_subdir.is_dir():
                rel = f"{l1_name}/{l2_name}/video"
                if rel not in done_paths:
                    tasks.append((l1_name, l2_name, "video", video_subdir, rel))
                continue

            # ── 标准情况：L3 子文件夹 ──
            for l3_dir in sorted(d for d in l2_dir.iterdir() if d.is_dir()):
                l3_name = l3_dir.name
                rel = f"{l1_name}/{l2_name}/{l3_name}"
                if rel not in done_paths:
                    tasks.append((l1_name, l2_name, l3_name, l3_dir, rel))

    return tasks


def _worker(args):
    """进程池工作函数"""
    l1_name, l2_name, l3_name, session_folder, rel = args
    return process_session(l1_name, l2_name, l3_name, session_folder, rel)


# ──────────────────────────── 主流程 ──────────────────────────────

def main():
    reset = "--reset" in sys.argv
    video_root = Path(VIDEO_ROOT)

    if reset:
        print("[RESET] 删除已有图片和索引...")
        if Path(OUTPUT_ROOT).exists():
            jpgs = list(Path(OUTPUT_ROOT).rglob("*.jpg")) + list(Path(OUTPUT_ROOT).rglob("*.JPG"))
            for jpg in jpgs:
                jpg.unlink(missing_ok=True)
            print(f"  已删除 {len(jpgs)} 张图片")
        if Path(INDEX_FILE).exists():
            Path(INDEX_FILE).unlink()
            print(f"  已删除 {INDEX_FILE}")

    # 加载已有索引（支持断点续跑）
    sessions = []
    done_paths = set()
    if Path(INDEX_FILE).exists():
        with open(INDEX_FILE, encoding="utf-8") as f:
            existing = json.load(f)
        sessions = existing.get("sessions", existing) if isinstance(existing, dict) else existing
        done_paths = {s["folder_path"] for s in sessions}
        print(f"[断点续跑] 已有 {len(done_paths)} 条记录")

    # Phase 1：收集任务
    print("Phase 1: 扫描目录结构...")
    tasks = collect_tasks(video_root, done_paths)
    print(f"  待处理 session 数: {len(tasks)}，使用 {NUM_WORKERS} 个进程")

    if tasks:
        # Phase 2：并行提取帧
        print("Phase 2: 并行提取帧...")
        SAVE_INTERVAL = 20
        completed = 0

        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(_worker, t): t[4] for t in tasks}
            for future in as_completed(futures):
                rel = futures[future]
                try:
                    session = future.result()
                except Exception as e:
                    print(f"  [ERROR] {rel}: {e}")
                    session = None

                if session:
                    sessions.append(session)
                    done_paths.add(rel)
                    print(f"  [OK] {rel}")

                completed += 1
                if completed % SAVE_INTERVAL == 0:
                    _save_index(sessions)
                    print(f"  [保存] {completed}/{len(tasks)} 完成")

    # Phase 3：生成日代表图索引
    print("Phase 3: 生成日代表图索引...")
    day_summaries = build_day_summaries(sessions)
    print(f"  共 {len(day_summaries)} 个 (场景, 日期) 组合")

    _save_index(sessions, day_summaries)

    # 统计汇报
    print(f"\n✅ 完成: {len(sessions)} 个 sessions，{len(day_summaries)} 个日期组")
    by_l1 = defaultdict(lambda: {"count": 0, "duration": 0.0})
    for s in sessions:
        by_l1[s["l1"]]["count"] += 1
        by_l1[s["l1"]]["duration"] += s["duration"]
    for k in sorted(by_l1):
        print(f"  {k}: {by_l1[k]['count']} sessions, {by_l1[k]['duration']:.1f} 分钟")


def _save_index(sessions: list, day_summaries: list = None):
    data = {"sessions": sessions}
    if day_summaries is not None:
        data["day_summaries"] = day_summaries
    elif Path(INDEX_FILE).exists():
        # 保留已有的 day_summaries
        with open(INDEX_FILE, encoding="utf-8") as f:
            old = json.load(f)
        if isinstance(old, dict) and "day_summaries" in old:
            data["day_summaries"] = old["day_summaries"]
    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
