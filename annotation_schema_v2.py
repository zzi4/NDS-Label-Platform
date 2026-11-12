"""Strict, isolated V2 annotations for one existing representative aerial frame.

This module has no worker, database, application configuration or model side
effects. V1 remains in annotation_schema.py; no V1 label is coerced to V2 here.
"""
from copy import deepcopy
import json
from pathlib import Path

SCHEMA_VERSION = 'aerial_annotation_v2'
PROMPT_VERSION = 'battery-frame-core-v2'
FIELD_DEFINITIONS = json.loads(
    (Path(__file__).resolve().parent / 'data' / 'annotation-fields-v2.json').read_text(encoding='utf-8')
)
STATES = ('observed', 'not_observed', 'unknown', 'not_applicable')
_FIELDS_BY_CODE = {item['code']: deepcopy(item) for item in FIELD_DEFINITIONS}
_REQUIRED = {'field', 'state', 'values', 'scope', 'origin', 'evidence'}
_ALLOWED = _REQUIRED | {'reason'}


def definitions() -> list[dict]:
    """Return an independent copy of the exact V2 field definitions."""
    return deepcopy(FIELD_DEFINITIONS)


def validate_annotation(payload: dict | list) -> list[str]:
    """Return contract errors without modifying, repairing or coercing input.

    Model responses use exactly ``{"labels": [...]}``. The bare labels array
    is also accepted for storage readers; both require all twelve V2 fields.
    Validation checks format, not whether the image supports an observation.
    """
    errors = []
    if isinstance(payload, dict):
        if set(payload) != {'labels'}:
            errors.append('annotation object must contain exactly the labels property')
        labels = payload.get('labels')
    else:
        labels = payload
    if not isinstance(labels, list):
        return errors + ['annotation labels must be an array']
    if len(labels) != len(_FIELDS_BY_CODE):
        errors.append('labels must contain exactly 12 fields')
    seen = set()
    for index, item in enumerate(labels):
        prefix = f'labels[{index}]'
        if not isinstance(item, dict):
            errors.append(f'{prefix}: label must be an object')
            continue
        missing, extra = _REQUIRED - set(item), set(item) - _ALLOWED
        if missing:
            errors.append(f'{prefix}: missing properties {", ".join(sorted(missing))}')
        if extra:
            errors.append(f'{prefix}: unknown properties {", ".join(sorted(map(str, extra)))}')
        code = item.get('field')
        definition = _FIELDS_BY_CODE.get(code) if isinstance(code, str) else None
        if definition is None:
            errors.append(f'{prefix}: unknown field {code!r}')
        else:
            prefix += f' ({code})'
            if code in seen:
                errors.append(f'{prefix}: duplicate field')
            seen.add(code)
        state = item.get('state')
        if not isinstance(state, str) or state not in STATES:
            errors.append(f'{prefix}: state must be one of {", ".join(STATES)}')
        for key, expected in (('scope', 'frame'), ('origin', 'model'), ('evidence', 'representative')):
            if item.get(key) != expected:
                errors.append(f'{prefix}: {key} must be {expected!r}')
        reason = item.get('reason')
        valid_reason = isinstance(reason, str) and bool(reason.strip())
        if 'reason' in item and not valid_reason:
            errors.append(f'{prefix}: reason must be a nonempty string when provided')
        if isinstance(state, str) and state in ('unknown', 'not_applicable') and not valid_reason:
            errors.append(f'{prefix}: {state} requires a reason')
        if code == 'scene.type' and state == 'observed' and not valid_reason:
            errors.append(f'{prefix}: observed scene requires a visible-evidence reason')
        values = item.get('values')
        if not isinstance(values, list):
            errors.append(f'{prefix}: values must be an array')
            continue
        if state == 'observed' and not values:
            errors.append(f'{prefix}: observed requires at least one value')
        elif isinstance(state, str) and state in STATES and state != 'observed' and values:
            errors.append(f'{prefix}: {state} requires empty values')
        if definition is not None:
            if not definition['multi'] and len(values) > 1:
                errors.append(f'{prefix}: single selection permits at most one value')
            selected = set()
            for value in values:
                if not isinstance(value, str) or value not in definition['values']:
                    errors.append(f'{prefix}: invalid enum value {value!r}')
                    continue
                if value in selected:
                    errors.append(f'{prefix}: duplicate value {value!r}')
                selected.add(value)
    for code in _FIELDS_BY_CODE:
        if code not in seen:
            errors.append(f'missing field: {code}')
    return errors


def extract_labels(payload: dict | list) -> list[dict]:
    """Copy strictly valid labels or raise ValueError; never silently project V1."""
    errors = validate_annotation(payload)
    if errors:
        raise ValueError('Invalid V2 annotation: ' + '; '.join(errors))
    return deepcopy(payload['labels'] if isinstance(payload, dict) else payload)


def build_annotation_prompt() -> str:
    """Build the fixed twelve-feature contract and conservative visual rules."""
    rules = '''你是一名无人机航拍道路图像标注员。输入只是一张现有代表帧。
仅依据本帧可见证据完成以下十二个核心特征。只输出一个 JSON 对象，禁止 Markdown 和额外解释。

【格式与四种状态】
1. 输出 {"labels": [...]}，恰好包含下方全部 12 个字段，各一次；不得遗漏、重复或混入旧版字段。
2. 每项必需且仅允许 field、state、values、scope、origin、evidence，以及可选 reason。
   scope 固定为 frame，origin 固定为 model，evidence 固定为 representative。values 始终是数组。
3. state 只能是 observed、not_observed、unknown、not_applicable；不得输出 unlabeled。
   observed 表示本帧有清晰正向可见依据，values 必须非空，且仅用字段定义的中文枚举。
   multi=false 的 observed 必须恰好一个值；multi=true 可多选不同正向值，不得重复。
   not_observed 仅用于相关区域清晰可见、确实未看到目标或设施时，values=[]。
   unknown 表示分辨率不足、遮挡、视角限制、歧义或证据不支持任何选项，values=[]。
   not_applicable 表示字段对主体场景不适用，values=[]。
   unknown、not_applicable 必须有非空 reason。scene.type 为 observed 时也必须有非空 reason，描述主体及可见判断依据。
   其他 reason 可选，提供时必须非空。不存在“未知”或“无”等正向选项；用状态和空数组表达。

【先确定主体，再选择一个场景】
4. 先确定占据画面主要观察范围、具有连贯结构的主体道路或主体区域，再在主体内部按顺序判断：
   环岛／十字路口 → 停车区域 → 园区道路 → 普通道路路段。不能由背景角落一个小路口或停车区覆盖主体。
   scene.type 为单选，只能选：高速路、快速路、城市主干路、城市街巷、十字路口、环岛、停车区域、园区道路。
   环岛须可见环形交通组织；十字路口须是主体的同层十字道路交汇。停车场内部通道的交叉不算十字路口；立交桥上下道路交叠也不算。
   停车区域须以成片停车空间、车位及其内部通行为主体；道路边少量路侧停车不使主体道路变成停车区域。
   园区道路须有可辨认的内部道路、建筑/场地服务关系或边界依据；不能仅凭有绿化判断园区。
5. 普通路段按可见结构及环境判断：高速路侧重分隔、封闭、匝道控制及非城市道路环境；快速路侧重城市内分隔、出入控制和连续通行。
   城市主干路用于有城市走廊、交通集散和沿线建成环境依据的主要/次要城市干道；本平台城市主干路可合并主干与次干道路场景。
   城市街巷用于可辨认的城市小尺度街巷、窄路及沿街服务环境。不能只凭车道数确定场景或法定道路等级。
   八类无法覆盖的普通道路、乡村道路、丁字或畸形交叉主体，以及多主体难以区分、类别证据冲突时，scene.type 用 unknown + [] 并说明依据不足。
   不把不确定强行归入城市街巷，不把 unknown 当成第九种场景，不多选。

【十二项的观察边界】
6. road.lane.count 只统计主体道路一个主要行驶方向当前可辨认断面的可见车道数量；不把对向、辅路、匝道或邻路相加。标线/道路边界不足则 unknown。
7. road.surface.material 用粗材质；road.surface.condition 用本帧可见表面状态，不能凭反光推断湿润、结冰或凭阴影推断路面材质。
   road.edge.type 可多选，但仅选主体道路清楚可见的边界；“无物理隔离”须边缘可见且确实无隔离。
8. traffic.signal 和 traffic.sign 只判断可见性，不猜标志内容。清晰可见的灯具/标志牌才 observed + ["有"]。
   小目标、背面、遮挡或分辨率不足时用 unknown，不能把看不清当成 not_observed。
   traffic.marking 仅选清楚可辨的实线、虚线、双黄线、导流线、斑马线、标线磨损；模糊标线用 unknown，不猜类型。
9. dynamic.motor_vehicle 仅选小型机动车、大型客车、货车、其他机动车等粗类；不猜车型、用途或特种身份。
   dynamic.vru 仅区分行人和骑行者；不细分自行车、电动车或三轮车。dynamic.obstacle 仅判断明显障碍物是否可见。
10. environment.light.intensity 仅描述画面可见光照；不推断天气、气候、温度、季节或光源。
    不从植被、衣着、反光、积雪或阴影猜天气、温度；不猜学校、医院等周边设施用途。
    不输出几何测量、坡度、横坡、宽度、曲率、速度、数值置信度或任何不在十二字段中的信息。
11. 所有结果只代表这一帧，不扩展为视频或整批次的出现次数、持续时间、覆盖范围或统计结论。
'''
    return (rules + '\n【字段定义 JSON】\n'
            + json.dumps(FIELD_DEFINITIONS, ensure_ascii=False, indent=2)
            + '\n逐项依据本帧独立判断；输出完整十二字段及固定来源，返回单个 JSON 对象。')
