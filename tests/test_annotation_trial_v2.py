"""Isolated bounded trials use real temporary images and injected transport only."""
from functools import lru_cache
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys

from PIL import Image
import pytest

PROJECT = Path(__file__).resolve().parents[1]
FIELD_CODES = [
    'scene.type', 'road.lane.count', 'road.surface.material',
    'road.surface.condition', 'road.edge.type', 'traffic.signal',
    'traffic.sign', 'traffic.marking', 'dynamic.motor_vehicle',
    'dynamic.vru', 'dynamic.obstacle', 'environment.light.intensity',
]


@lru_cache(maxsize=1)
def runner():
    path = PROJECT / 'annotation_trial_v2.py'
    assert path.is_file(), 'Missing isolated V2 trial runner'
    spec = importlib.util.spec_from_file_location('annotation_trial_v2', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def complete_response():
    payload = {'labels': [
        {'field': code, 'state': 'unknown', 'values': [], 'scope': 'frame',
         'origin': 'model', 'evidence': 'representative', 'reason': '无法判断'}
        for code in FIELD_CODES
    ]}
    return {'raw_content': json.dumps(payload, ensure_ascii=False),
            'model': 'deepseek-flash', 'usage': {'prompt_tokens': 2, 'completion_tokens': 3, 'total_tokens': 5}}


def manifest(tmp_path, count=1):
    rows = []
    for index in range(count):
        frame = tmp_path / f'input-{index}.jpg'
        Image.new('RGB', (16, 12), (60, 80, 100)).save(frame)
        rows.append({'id': f'sample-{index}', 'frame_path': str(frame)})
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(rows))
    path.chmod(0o600)
    return path


def forbid_database(*args, **kwargs):
    pytest.fail('An isolated trial attempted database access or full worker configuration')


def test_trial_sends_one_call_per_sample_with_v2_prompt_budget_and_saves_private_evidence(tmp_path, monkeypatch, capsys):
    trial = runner()
    import annotation_worker as worker
    monkeypatch.setattr(worker.Config, 'environment', forbid_database)
    monkeypatch.setattr(worker.psycopg, 'connect', forbid_database)
    source = manifest(tmp_path, 2)
    before = {path: path.read_bytes() for path in tmp_path.glob('input-*')}
    calls = []
    def client(config, frame, *, prompt, max_tokens):
        assert config.api_key == 'fake-key'
        assert not hasattr(config, 'writer_dsn')
        assert 'fake-key' not in repr(config)
        calls.append((frame, prompt, max_tokens))
        return complete_response()
    results = trial.run_trials(source, tmp_path / 'results', api_key='fake-key', client=client)
    assert [r['status'] for r in results] == ['valid', 'valid']
    assert len(calls) == 2
    assert all(budget == 4096 for _, _, budget in calls)
    assert all('【字段定义 JSON】' in prompt and 'scene.type' in prompt for _, prompt, _ in calls)
    assert {path: path.read_bytes() for path in before} == before
    result_dir = tmp_path / 'results'
    assert stat.S_IMODE(result_dir.stat().st_mode) == 0o700
    saved = json.loads((result_dir / 'sample-0.json').read_text())
    assert saved['raw_content'] == complete_response()['raw_content']
    assert saved['schema_version'] == 'aerial_annotation_v2'
    assert saved['prompt_version'] == 'battery-frame-core-v2.1'
    assert len(saved['prompt_sha256']) == 64
    assert len(saved['frame_sha256']) == 64
    assert saved['usage'] == {'prompt_tokens': 2, 'completion_tokens': 3, 'total_tokens': 5}
    assert saved['validation_errors'] == []
    assert [label['field'] for label in saved['labels']] == FIELD_CODES
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in result_dir.iterdir())
    summary = json.loads((result_dir / 'run.json').read_text())
    assert summary['attempted'] == 2
    assert summary['stopped_fatal'] is False
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert output == [{'id': 'sample-0', 'status': 'valid'}, {'id': 'sample-1', 'status': 'valid'}]


def test_default_eight_call_ceiling_and_smaller_requested_limit(tmp_path):
    trial = runner()
    source = manifest(tmp_path, 10)
    calls = []
    def client(config, frame, **kwargs):
        calls.append(frame)
        return complete_response()
    assert len(trial.run_trials(source, tmp_path / 'eight', api_key='x', client=client)) == 8
    assert len(calls) == 8
    calls.clear()
    assert len(trial.run_trials(source, tmp_path / 'two', api_key='x', max_frames=2, client=client)) == 2
    assert len(calls) == 2


@pytest.mark.parametrize('maximum', [0, 9, -1, True, 1.5])
def test_invalid_call_limits_fail_before_any_output_or_call(tmp_path, maximum):
    trial = runner()
    source = manifest(tmp_path)
    with pytest.raises(ValueError):
        trial.run_trials(source, tmp_path / 'results', api_key='x', max_frames=maximum, client=forbid_database)
    assert not (tmp_path / 'results').exists()


@pytest.mark.parametrize('code,fatal_flag', [('api_auth', False), ('api_quota', False), ('api_unavailable', True)])
def test_auth_quota_or_explicit_fatal_stops_after_one_call_and_retains_error(tmp_path, code, fatal_flag, capsys):
    trial = runner()
    from annotation_worker import APIError
    source = manifest(tmp_path, 3)
    calls = []
    def client(config, frame, **kwargs):
        calls.append(frame)
        raise APIError(code, fatal=fatal_flag)
    results = trial.run_trials(source, tmp_path / 'results', api_key='fake-key', client=client)
    assert len(results) == len(calls) == 1
    saved = json.loads((tmp_path / 'results/sample-0.json').read_text())
    assert saved['error_code'] == code
    assert saved['fatal'] is True
    assert saved['raw_content'] is None
    summary = json.loads((tmp_path / 'results/run.json').read_text())
    assert summary['stopped_fatal'] is True
    assert 'fake-key' not in capsys.readouterr().out


def test_rate_limit_records_once_and_continues_without_retry(tmp_path):
    trial = runner()
    from annotation_worker import APIError
    source = manifest(tmp_path, 2)
    calls = []
    def client(config, frame, **kwargs):
        calls.append(frame)
        if len(calls) == 1:
            raise APIError('api_rate_limit')
        return complete_response()
    results = trial.run_trials(source, tmp_path / 'results', api_key='x', client=client)
    assert [r['status'] for r in results] == ['api_error', 'valid']
    assert len(calls) == 2


@pytest.mark.parametrize('raw,expected', [('not json', 'invalid_json'), ('{"labels": []}', 'validation_failed'), ('[]', 'validation_failed')])
def test_malformed_outputs_are_preserved_and_never_repaired_or_retried(tmp_path, raw, expected):
    trial = runner()
    source = manifest(tmp_path)
    calls = []
    def client(config, frame, **kwargs):
        calls.append(frame)
        return {'raw_content': raw, 'model': 'deepseek-flash', 'usage': {'total_tokens': 7}}
    results = trial.run_trials(source, tmp_path / 'results', api_key='x', client=client)
    assert len(calls) == 1
    saved = json.loads((tmp_path / 'results/sample-0.json').read_text())
    assert results[0]['status'] == saved['status'] == expected
    assert saved['raw_content'] == raw
    assert saved['validation_errors']
    assert saved['labels'] is None


@pytest.mark.parametrize('replacement', [[], {}, [{'id': '../escape', 'frame_path': '/not-an-image'}],
                                        [{'id': True, 'frame_path': '/not-an-image'}],
                                        [{'id': 'run', 'frame_path': '/not-an-image'}],
                                        [{'id': 'one', 'frame_path': '/missing'}]])
def test_manifest_preflight_rejects_invalid_or_missing_inputs_without_output(tmp_path, replacement):
    trial = runner()
    source = tmp_path / 'manifest.json'
    source.write_text(json.dumps(replacement))
    source.chmod(0o600)
    with pytest.raises(ValueError):
        trial.run_trials(source, tmp_path / 'results', api_key='x', client=forbid_database)
    assert not (tmp_path / 'results').exists()


def test_duplicate_ids_and_non_images_are_rejected_before_calls(tmp_path):
    trial = runner()
    source = manifest(tmp_path, 2)
    rows = json.loads(source.read_text())
    rows[1]['id'] = rows[0]['id']
    source.write_text(json.dumps(rows))
    with pytest.raises(ValueError):
        trial.run_trials(source, tmp_path / 'results', api_key='x', client=forbid_database)
    rows[1]['id'] = 'second'
    Path(rows[1]['frame_path']).write_text('not an image')
    source.write_text(json.dumps(rows))
    with pytest.raises(ValueError):
        trial.run_trials(source, tmp_path / 'results', api_key='x', client=forbid_database)
    assert not (tmp_path / 'results').exists()


def test_public_manifest_or_output_permissions_are_refused_before_calls(tmp_path):
    trial = runner()
    source = manifest(tmp_path)
    source.chmod(0o644)
    with pytest.raises(ValueError):
        trial.run_trials(source, tmp_path / 'results', api_key='x', client=forbid_database)
    source.chmod(0o600)
    output = tmp_path / 'results'
    output.mkdir(mode=0o755)
    output.chmod(0o755)
    with pytest.raises(ValueError):
        trial.run_trials(source, output, api_key='x', client=forbid_database)
    assert list(output.iterdir()) == []


def test_existing_results_are_not_overwritten_or_recalled(tmp_path):
    trial = runner()
    source = manifest(tmp_path)
    output = tmp_path / 'results'
    output.mkdir(mode=0o700)
    original = output / 'sample-0.json'
    original.write_text('prior evidence')
    with pytest.raises(ValueError):
        trial.run_trials(source, output, api_key='x', client=forbid_database)
    assert original.read_text() == 'prior evidence'


def test_secret_in_model_content_and_exception_text_never_leaks(tmp_path, capsys):
    trial = runner()
    source = manifest(tmp_path, 2)
    calls = []
    def client(config, frame, **kwargs):
        calls.append(frame)
        if len(calls) == 1:
            raise RuntimeError('fake-secret /private/location and DSN')
        return {'raw_content': 'fake-secret', 'model': 'deepseek-flash',
                'usage': {'total_tokens': True, 'credential': 'fake-secret'}}
    results = trial.run_trials(source, tmp_path / 'results', api_key='fake-secret', client=client)
    assert results[0]['error_code'] == 'client_error'
    assert results[1]['raw_content'] == '[redacted]'
    text = capsys.readouterr().out
    assert 'fake-secret' not in text
    assert '/private' not in text
    assert all('fake-secret' not in path.read_text() for path in (tmp_path / 'results').iterdir())
    assert results[1]['usage'] == {}


def test_cli_uses_safe_env_loader_minimal_config_and_reused_worker_transport(tmp_path, monkeypatch, capsys):
    trial = runner()
    import annotation_worker as worker
    source = manifest(tmp_path)
    secret = tmp_path / 'fake.env'
    secret.write_text('DEEPSEEK_API_KEY=fake-cli-key\n')
    secret.chmod(0o600)
    monkeypatch.setenv('DEEPSEEK_API_KEY', '')
    monkeypatch.delenv('DEEPSEEK_API_KEY', raising=False)
    monkeypatch.delenv('DEEPSEEK_VISION_MODEL', raising=False)
    monkeypatch.setattr(worker.Config, 'environment', forbid_database)
    monkeypatch.setattr(worker.psycopg, 'connect', forbid_database)
    calls = []
    def client(config, frame, *, prompt, max_tokens):
        calls.append(config.api_key)
        return complete_response()
    monkeypatch.setattr(worker, 'call_model', client)
    assert trial.main(['--env-file', str(secret), '--manifest', str(source), '--output', str(tmp_path / 'results')]) == 0
    assert calls == ['fake-cli-key']
    assert 'fake-cli-key' not in capsys.readouterr().out


def test_cli_refuses_world_readable_env_without_loading_or_calling(tmp_path, monkeypatch):
    trial = runner()
    import annotation_worker as worker
    source = manifest(tmp_path)
    secret = tmp_path / 'fake.env'
    secret.write_text('DEEPSEEK_API_KEY=must-not-load\n')
    secret.chmod(0o644)
    monkeypatch.setenv('DEEPSEEK_API_KEY', '')
    monkeypatch.delenv('DEEPSEEK_API_KEY', raising=False)
    monkeypatch.setattr(worker, 'call_model', forbid_database)
    assert trial.main(['--env-file', str(secret), '--manifest', str(source), '--output', str(tmp_path / 'results')]) == 2
    assert 'DEEPSEEK_API_KEY' not in os.environ
    assert not (tmp_path / 'results').exists()


def test_trial_rejects_duplicate_json_properties_instead_of_silently_selecting_last(tmp_path):
    trial = runner()
    source = manifest(tmp_path)
    raw = complete_response()['raw_content'].replace('"state": "unknown"', '"state": "observed", "state": "unknown"', 1)
    def client(config, frame, **kwargs):
        return {'raw_content': raw, 'model': 'deepseek-flash', 'usage': {}}
    result = trial.run_trials(source, tmp_path / 'results', api_key='fake-key', client=client)[0]
    assert result['status'] == 'invalid_json'
    assert result['raw_content'] == raw


def test_non_jpeg_image_is_refused_before_jpeg_only_transport(tmp_path):
    trial = runner()
    source = manifest(tmp_path)
    image = tmp_path / 'png-frame.png'
    Image.new('RGB', (16, 12)).save(image)
    source.write_text(json.dumps([{'id': 'one', 'frame_path': str(image)}]))
    with pytest.raises(ValueError):
        trial.run_trials(source, tmp_path / 'results', api_key='x', client=forbid_database)
    assert not (tmp_path / 'results').exists()


def test_reserved_run_identifier_is_rejected_before_creating_run_json(tmp_path):
    trial = runner()
    source = manifest(tmp_path)
    rows = json.loads(source.read_text())
    rows[0]['id'] = 'run'
    source.write_text(json.dumps(rows))
    with pytest.raises(ValueError):
        trial.run_trials(source, tmp_path / 'results', api_key='x', client=forbid_database)
    assert not (tmp_path / 'results').exists()
