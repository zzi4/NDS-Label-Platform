"""V2 frame contract regressions; no application settings or model requests."""
from copy import deepcopy
from functools import lru_cache
import importlib.util
import json
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]
FIELD_CODES = [
    'scene.type', 'road.lane.count', 'road.surface.material',
    'road.surface.condition', 'road.edge.type', 'traffic.signal',
    'traffic.sign', 'traffic.marking', 'dynamic.motor_vehicle',
    'dynamic.vru', 'dynamic.obstacle', 'environment.light.intensity',
]
SCENES = ['高速路', '快速路', '城市主干路', '城市街巷', '十字路口', '环岛', '停车区域', '园区道路']


@lru_cache(maxsize=1)
def contract():
    path = PROJECT / 'annotation_schema_v2.py'
    assert path.is_file(), 'Missing V2 frame contract'
    spec = importlib.util.spec_from_file_location('annotation_schema_v2', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def unknown_frame():
    return {'labels': [
        {'field': code, 'state': 'unknown', 'values': [], 'scope': 'frame',
         'origin': 'model', 'evidence': 'representative', 'reason': '画面无法判断'}
        for code in FIELD_CODES
    ]}


def observe(payload, code, values, reason=None):
    item = next(item for item in payload['labels'] if item['field'] == code)
    item.update(state='observed', values=values)
    item.pop('reason', None)
    if reason is not None:
        item['reason'] = reason
    return item


def test_complete_twelve_field_envelope_and_stored_array_are_accepted_without_mutation():
    schema = contract()
    payload = unknown_frame()
    assert schema.validate_annotation(payload) == []
    assert schema.validate_annotation(payload['labels']) == []
    labels = schema.extract_labels(payload)
    assert [label['field'] for label in labels] == FIELD_CODES
    labels[0]['values'].append('高速路')
    assert payload['labels'][0]['values'] == []
    assert schema.SCHEMA_VERSION == 'aerial_annotation_v2'
    assert schema.PROMPT_VERSION == 'battery-frame-core-v2'


def test_taxonomy_exposes_exact_twelve_codes_enums_and_cardinality():
    expected = [
        ('scene.type', SCENES, False),
        ('road.lane.count', ['单车道', '双车道', '三车道', '四车道及以上'], False),
        ('road.surface.material', ['沥青', '混凝土', '未铺装', '其他铺装'], False),
        ('road.surface.condition', ['干燥', '潮湿', '积水', '积雪', '结冰', '泥泞'], False),
        ('road.edge.type', ['路缘石', '护栏 (金属)', '护栏 (混凝土)', '草地/泥土', '无物理隔离'], True),
        ('traffic.signal', ['有'], False), ('traffic.sign', ['有'], False),
        ('traffic.marking', ['实线', '虚线', '双黄线', '导流线', '斑马线', '标线磨损'], True),
        ('dynamic.motor_vehicle', ['小型机动车', '大型客车', '货车', '其他机动车'], True),
        ('dynamic.vru', ['行人', '骑行者'], True), ('dynamic.obstacle', ['有'], False),
        ('environment.light.intensity', ['正常', '强光/逆光', '弱光/昏暗', '黑暗'], False),
    ]
    schema = contract()
    assert [(f['code'], f['values'], f['multi']) for f in schema.FIELD_DEFINITIONS] == expected
    copied = schema.definitions()
    assert copied == schema.FIELD_DEFINITIONS
    copied[0]["values"].append("invented")
    assert schema.definitions()[0]["values"] == SCENES
    assert all(f['unobservable'] is False for f in schema.FIELD_DEFINITIONS)
    assert json.loads((PROJECT / 'data/annotation-fields-v2.json').read_text()) == schema.FIELD_DEFINITIONS


@pytest.mark.parametrize('scene', SCENES)
def test_each_scene_is_valid_with_one_value_and_visible_evidence(scene):
    payload = unknown_frame()
    observe(payload, 'scene.type', [scene], '主体结构清晰可见，符合该场景')
    assert contract().validate_annotation(payload) == []


@pytest.mark.parametrize('values', [[], ['城市主干路', '十字路口'], ['未知'], ['乡村道路'], [True]])
def test_scene_requires_exactly_one_of_eight_positive_values(values):
    payload = unknown_frame()
    observe(payload, 'scene.type', values, '可见主体道路')
    assert contract().validate_annotation(payload)


def test_scene_observed_requires_nonempty_visible_evidence_reason():
    payload = unknown_frame()
    item = observe(payload, 'scene.type', ['城市主干路'])
    for reason in (None, '', ' ', True, 1):
        if reason is None:
            item.pop('reason', None)
        else:
            item['reason'] = reason
        assert contract().validate_annotation(payload)


@pytest.mark.parametrize('state', ['unknown', 'not_applicable'])
def test_uncertainty_requires_empty_values_and_nonempty_reason(state):
    payload = unknown_frame()
    item = payload['labels'][5]
    item.update(state=state, values=[])
    assert contract().validate_annotation(payload) == []
    item.pop('reason')
    assert contract().validate_annotation(payload)
    item['reason'] = '遮挡'
    item['values'] = ['有']
    assert contract().validate_annotation(payload)


def test_positive_enums_and_not_observed_absence_are_not_interchangeable():
    payload = unknown_frame()
    item = observe(payload, 'traffic.sign', ['有'])
    assert contract().validate_annotation(payload) == []
    item['values'] = ['无']
    assert contract().validate_annotation(payload)
    item.update(state='not_observed', values=[])
    assert contract().validate_annotation(payload) == []
    item['values'] = ['有']
    assert contract().validate_annotation(payload)


@pytest.mark.parametrize('key,value', [
    ('state', 'unlabeled'), ('state', True), ('state', []),
    ('field', True), ('field', []), ('values', True), ('values', ['有', '有']),
    ('values', [True]), ('values', [1]), ('values', [{}]), ('values', '有'),
    ('scope', 'batch'), ('origin', 'human'), ('evidence', 'all_frames'),
    ('reason', ''), ('reason', True), ('confidence', 0.99), ('meters', 3),
])
def test_invalid_states_types_values_extras_and_provenance_are_rejected(key, value):
    payload = unknown_frame()
    item = observe(payload, 'traffic.signal', ['有'])
    item[key] = value
    assert contract().validate_annotation(payload)


@pytest.mark.parametrize('key', ['field', 'state', 'values', 'scope', 'origin', 'evidence'])
def test_all_fixed_label_properties_are_required(key):
    payload = unknown_frame()
    payload['labels'][0].pop(key)
    assert contract().validate_annotation(payload)


@pytest.mark.parametrize('payload', [None, True, 1, {}, '[]', {'labels': True}, {'labels': {}}, [True]])
def test_invalid_root_and_label_shapes_do_not_crash(payload):
    assert contract().validate_annotation(payload)


def test_missing_duplicate_extra_and_old_fields_reject_mixed_versions():
    payload = unknown_frame()
    payload['labels'].pop()
    assert any('environment.light.intensity' in error for error in contract().validate_annotation(payload))
    payload = unknown_frame()
    payload['labels'][-1] = deepcopy(payload['labels'][0])
    assert any('duplicate' in error for error in contract().validate_annotation(payload))
    payload = unknown_frame()
    payload['labels'].append({'field': 'environment.weather', 'state': 'unknown', 'values': [],
                              'scope': 'frame', 'origin': 'model', 'evidence': 'representative', 'reason': '无证据'})
    assert contract().validate_annotation(payload)
    payload = unknown_frame()
    payload['schema_version'] = 'aerial_annotation_v2'
    assert contract().validate_annotation(payload)
    with pytest.raises(ValueError):
        contract().extract_labels(payload)


def test_single_selection_limits_and_genuine_multiple_values():
    payload = unknown_frame()
    item = observe(payload, 'road.surface.material', ['沥青', '混凝土'])
    assert contract().validate_annotation(payload)
    item['values'] = ['未铺装']
    observe(payload, 'road.edge.type', ['路缘石', '草地/泥土'])
    observe(payload, 'traffic.marking', ['实线', '虚线'])
    observe(payload, 'dynamic.motor_vehicle', ['小型机动车', '货车'])
    observe(payload, 'dynamic.vru', ['行人', '骑行者'])
    assert contract().validate_annotation(payload) == []
    item = observe(payload, 'traffic.marking', ['实线', '实线'])
    assert contract().validate_annotation(payload)


def test_prompt_contains_parseable_exact_taxonomy_for_all_twelve_fields():
    prompt = contract().build_annotation_prompt()
    decoder = json.JSONDecoder()
    definitions = decoder.raw_decode(prompt.split('【字段定义 JSON】\n', 1)[1])[0]
    assert definitions == contract().FIELD_DEFINITIONS
    assert [item['code'] for item in definitions] == FIELD_CODES
    assert definitions[0]['multi'] is False
    assert definitions[0]['values'] == SCENES
