"""Private, bounded V2 trials on existing JPEG frames; no database or queue use.

The default transport is annotation_worker.call_model, using only an api_key.
Importing or running this module never invokes worker Config.environment.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile

from PIL import Image

import annotation_worker as worker
from annotation_schema_v2 import (
    SCHEMA_VERSION, PROMPT_VERSION, build_annotation_prompt, extract_labels,
    validate_annotation,
)

MAX_FRAMES = 8
MAX_TOKENS = 4096
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}\Z')
_API_CODES = {'api_auth', 'api_quota', 'api_rate_limit', 'api_unavailable', 'api_timeout', 'invalid_json'}


@dataclass(frozen=True)
class TrialConfig:
    """Only the credential required by the existing HTTP transport."""
    api_key: str = field(repr=False)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _private(path: Path, *, directory: bool = False) -> None:
    info = path.lstat()
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if not kind(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('Trial paths must be owned private files/directories without symlinks')


def _pairs_without_duplicates(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError('Duplicate JSON property')
        result[name] = value
    return result


def _no_constant(value):
    raise ValueError('Nonfinite JSON number')


def _json_loads(text: str):
    return json.loads(text, object_pairs_hook=_pairs_without_duplicates, parse_constant=_no_constant)


def _manifest(path: Path) -> list[dict]:
    """Preflight the complete private manifest before output or paid requests."""
    _private(path)
    rows = _json_loads(path.read_text(encoding='utf-8'))
    if not isinstance(rows, list) or not rows:
        raise ValueError('Manifest must be a nonempty JSON array')
    prepared, seen = [], set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'id', 'frame_path'}:
            raise ValueError('Manifest entries require exactly id and frame_path')
        identifier, raw_path = row['id'], row['frame_path']
        if (not isinstance(identifier, str) or not _ID.fullmatch(identifier)
                or identifier == 'run' or identifier in seen):
            raise ValueError('Manifest identifiers must be unique safe names of at most 80 characters')
        seen.add(identifier)
        if not isinstance(raw_path, str) or not raw_path:
            raise ValueError('Manifest frame_path must be an absolute existing JPEG file')
        frame = Path(raw_path)
        try:
            if not frame.is_absolute() or not frame.is_file():
                raise ValueError('Missing existing JPEG frame')
            with Image.open(frame) as image:
                if image.format != 'JPEG':
                    raise ValueError('Existing transport requires JPEG frames')
                image.verify()
            digest = hashlib.sha256(frame.read_bytes()).hexdigest()
        except (OSError, ValueError, Image.DecompressionBombError):
            raise ValueError('Manifest frame must be a readable existing JPEG image') from None
        prepared.append({'id': identifier, 'frame_path': frame, 'frame_sha256': digest})
    return prepared


def _write_json(path: Path, value: dict, *, exclusive: bool = False) -> None:
    """Create mode-0600 evidence, with atomic replacement for later updates."""
    content = json.dumps(value, ensure_ascii=False, indent=2) + '\n'
    if exclusive:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            stream.write(content)
        return
    descriptor, temporary = tempfile.mkstemp(prefix='.trial-', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            stream.write(content)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def run_trials(manifest: Path | str, output: Path | str, *, api_key: str,
               max_frames: int = MAX_FRAMES, client=None) -> list[dict]:
    """Call an injected/default HTTP client once per selected existing frame.

    Both preflight and every result are local. There are no retries, extraction,
    database connections, annotation writes or queue changes. Auth/quota errors
    stop immediately. A prior run.json or selected result prevents re-execution
    into the same directory. The returned evidence matches the private files.
    ``client(config, frame, *, prompt, max_tokens)`` returns the worker response.
    """
    if type(max_frames) is not int or not 1 <= max_frames <= MAX_FRAMES:
        raise ValueError('max_frames must be an integer from 1 through 8')
    if not isinstance(api_key, str) or not api_key.strip():
        raise ValueError('A nonempty DeepSeek API key is required')
    selected = _manifest(Path(manifest))[:max_frames]
    destination = Path(output)
    if destination.exists() or destination.is_symlink():
        _private(destination, directory=True)
        if (destination / 'run.json').exists() or (destination / 'run.json').is_symlink():
            raise ValueError('Trial output already contains run evidence')
        if any((destination / (row['id'] + '.json')).exists()
               or (destination / (row['id'] + '.json')).is_symlink() for row in selected):
            raise ValueError('Trial output already contains sample evidence')
    else:
        destination.mkdir(mode=0o700, parents=True)
        _private(destination, directory=True)
    prompt = build_annotation_prompt()
    digest = hashlib.sha256(prompt.encode('utf-8')).hexdigest()
    config = TrialConfig(api_key)
    transport = worker.call_model if client is None else client
    summary = {'schema_version': SCHEMA_VERSION, 'prompt_version': PROMPT_VERSION,
               'prompt_sha256': digest, 'model': worker.MODEL, 'max_tokens': MAX_TOKENS,
               'requested_max_frames': max_frames, 'selected': len(selected), 'attempted': 0,
               'started_at': _now(), 'finished_at': None, 'stopped_fatal': False, 'results': []}
    # Exclusive reservation also prevents two local processes calling the same run.
    try:
        _write_json(destination / 'run.json', summary, exclusive=True)
    except FileExistsError:
        raise ValueError('Trial output already contains run evidence') from None
    results = []
    for row in selected:
        record = {'id': row['id'], 'frame_path': str(row['frame_path']),
                  'frame_sha256': row['frame_sha256'], 'schema_version': SCHEMA_VERSION,
                  'prompt_version': PROMPT_VERSION, 'prompt_sha256': digest,
                  'model': worker.MODEL, 'max_tokens': MAX_TOKENS, 'started_at': _now(),
                  'finished_at': None, 'status': 'started', 'raw_content': None,
                  'usage': {}, 'validation_errors': [], 'labels': None,
                  'error_code': None, 'fatal': False}
        result_path = destination / (row['id'] + '.json')
        _write_json(result_path, record, exclusive=True)
        try:
            response = transport(config, row['frame_path'], prompt=prompt, max_tokens=MAX_TOKENS)
            if not isinstance(response, dict):
                raise ValueError('Malformed transport response')
            raw = response.get('raw_content')
            if not isinstance(raw, str):
                raise ValueError('Missing model response content')
            record['raw_content'] = raw.replace(api_key, '[redacted]')
            usage = response.get('usage', {})
            if isinstance(usage, dict):
                record['usage'] = {name: value for name, value in usage.items()
                                   if name in {'prompt_tokens', 'completion_tokens', 'total_tokens'}
                                   and type(value) is int and value >= 0}
            # Persist received content before parsing; malformed labels remain evidence.
            _write_json(result_path, record)
            if response.get('model') != worker.MODEL:
                record.update(status='client_error', error_code='unexpected_model')
            else:
                try:
                    payload = _json_loads(record['raw_content'])
                except (ValueError, RecursionError):
                    record.update(status='invalid_json', error_code='invalid_json',
                                  validation_errors=['Model content is not strict JSON'])
                else:
                    # The wire contract requires an envelope; the schema also serves
                    # stored bare arrays, which are intentionally rejected here.
                    errors = validate_annotation(payload)
                    if not isinstance(payload, dict):
                        errors.insert(0, 'Model content must be an object containing labels')
                    record['validation_errors'] = errors
                    if errors:
                        record.update(status='validation_failed', error_code='validation_failed')
                    else:
                        record.update(status='valid', labels=extract_labels(payload))
        except worker.APIError as error:
            code = error.code if error.code in _API_CODES else 'api_error'
            record.update(status='api_error', error_code=code,
                          fatal=bool(error.fatal) or code in {'api_auth', 'api_quota'})
        except Exception:
            # Exception strings can contain credentials, response bodies or paths.
            record.update(status='client_error', error_code='client_error')
        record['finished_at'] = _now()
        _write_json(result_path, record)
        results.append(record)
        summary['attempted'] = len(results)
        summary['results'].append({'id': record['id'], 'status': record['status']})
        summary['stopped_fatal'] = record['fatal']
        _write_json(destination / 'run.json', summary)
        print(json.dumps({'id': record['id'], 'status': record['status']}), flush=True)
        if record['fatal']:
            break
    summary['finished_at'] = _now()
    _write_json(destination / 'run.json', summary)
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Run private V2 trials on at most eight existing JPEG frames')
    parser.add_argument('--env-file', type=Path, help='Private configuration read with the existing safe worker loader')
    parser.add_argument('--manifest', type=Path, required=True, help='Private JSON array of id/frame_path entries')
    parser.add_argument('--output', type=Path, required=True, help='New or empty private output directory')
    parser.add_argument('--max-frames', type=int, choices=range(1, MAX_FRAMES + 1), default=MAX_FRAMES)
    args = parser.parse_args(argv)
    try:
        if args.env_file is not None:
            _private(args.env_file)
            worker.load_env_file(args.env_file)
        if os.environ.get('DEEPSEEK_VISION_MODEL', worker.MODEL) != worker.MODEL:
            raise ValueError('Unsupported model configuration')
        results = run_trials(args.manifest, args.output, api_key=os.environ.get('DEEPSEEK_API_KEY', ''),
                             max_frames=args.max_frames)
    except (OSError, ValueError):
        print(json.dumps({'status': 'configuration_error'}), file=sys.stderr)
        return 2
    return 0 if all(result['status'] == 'valid' for result in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
