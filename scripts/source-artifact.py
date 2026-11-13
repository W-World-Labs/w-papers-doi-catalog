#!/usr/bin/env python3
"""Archive exact reviewed Git source and CI Python dependency declarations."""
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import unicodedata
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def main():
    report = json.loads(Path(sys.argv[1]).read_text())
    output = Path(sys.argv[2]).resolve()
    if output == ROOT or ROOT in output.parents:
        raise ValueError('Output must be outside source')
    output.mkdir(parents=True, exist_ok=False)
    provenance = json.loads((ROOT / 'source-provenance.json').read_text())['files']
    expected = set(provenance) | {'source-provenance.json'}
    files, normalized, total = {}, {}, 0
    for row in filter(None, git('ls-tree', '-rz', 'HEAD').split(b'\0')):
        attrs, raw_name = row.split(b'\t', 1)
        mode, kind, oid = attrs.decode().split()
        name = raw_name.decode()
        if kind != 'blob' or mode not in {'100644', '100755'}:
            raise ValueError('Unreviewed source type: ' + name)
        parts = Path(name).parts
        for i in range(1, len(parts) + 1):
            prefix = '/'.join(parts[:i])
            key = unicodedata.normalize('NFC', prefix).casefold()
            if normalized.setdefault(key, prefix) != prefix:
                raise ValueError('Case/Unicode collision')
        data = git('cat-file', 'blob', oid)
        path = ROOT / name
        if path.is_symlink() or not path.is_file() or path.read_bytes() != data:
            raise ValueError('Source differs from Git: ' + name)
        item = {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
        if name != 'source-provenance.json' and provenance[name] != item:
            raise ValueError('Provenance differs: ' + name)
        if len(data) > 10 * 1024 * 1024:
            raise ValueError('Source blob exceeds limit')
        files[name] = item
        total += len(data)
    if set(files) != expected or total > 100 * 1024 * 1024:
        raise ValueError('Source closure differs')
    packages = []
    for entry in report['install']:
        meta = entry['metadata']
        info = entry['download_info']
        url = urlsplit(info['url'])
        if (url.scheme != 'https' or url.hostname != 'files.pythonhosted.org'
                or url.username or url.password or url.query or url.fragment):
            raise ValueError('Dependency is not from public PyPI')
        license_value = meta.get('license_expression') or meta.get('license')
        classifiers = [x for x in meta.get('classifier', meta.get('classifiers', []))
                       if x.startswith('License ::')]
        license_files = meta.get('license_file', meta.get('license_files', []))
        if not (license_value or classifiers or license_files):
            raise ValueError('Missing license declaration: ' + meta['name'])
        sha = info['archive_info']['hashes']['sha256']
        if not re.fullmatch('[0-9a-f]{64}', sha):
            raise ValueError('Missing dependency archive hash')
        packages.append({'name': meta['name'], 'version': meta['version'],
                         'url': info['url'], 'sha256': sha,
                         'license': license_value, 'license_classifiers': classifiers,
                         'license_files': license_files})
    normalize = lambda value: re.sub('[-_.]+', '-', value).lower()
    resolved = {normalize(x['name']): x['version'] for x in packages}
    for line in (ROOT / 'requirements.txt').read_text().splitlines():
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        name, version = line.split('==')
        if resolved[normalize(name.split('[')[0])] != version:
            raise ValueError('Direct requirement differs')
    revision = git('rev-parse', 'HEAD').decode().strip()
    if revision != os.environ['GITHUB_SHA']:
        raise ValueError('Checkout differs from CI SHA')
    raw = git('archive', '--format=tar', 'HEAD')
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        members = {m.name: m for m in archive.getmembers() if m.isfile()}
        if set(members) != expected:
            raise ValueError('Archive source closure differs')
        for name, item in files.items():
            if hashlib.sha256(archive.extractfile(members[name]).read()).hexdigest() != item['sha256']:
                raise ValueError('Archive bytes differ')
    names = ['w-papers-doi-catalog-source.tar.gz', 'SOURCE.json', 'THIRD-PARTY-DEPENDENCIES.json']
    (output / names[0]).write_bytes(gzip.compress(raw, mtime=0))
    (output / names[1]).write_text(json.dumps({
        'commit': revision, 'tree': git('rev-parse', 'HEAD^{tree}').decode().strip(),
        'repository': os.environ['GITHUB_REPOSITORY'], 'files': files,
        'scope': 'complete independent DOI/cache/OpenAlex program source and original synthetic verification',
        'licenses': ['LICENSE', 'AUTHORS.md', 'NOTICE.md'],
        'compiled_binaries_included': False, 'dataset_included': False,
        'data_coverage_acceptance': False, 'production_activation': False}, indent=2) + '\n')
    (output / names[2]).write_text(json.dumps({
        'scope': 'resolved public PyPI declarations for this Linux Python 3.12 CI run; no dependency code; not a portable lockfile',
        'packages': sorted(packages, key=lambda x: normalize(x['name']))}, indent=2) + '\n')
    (output / 'SHA256SUMS').write_text(''.join(
        hashlib.sha256((output / n).read_bytes()).hexdigest() + '  ' + n + '\n' for n in names))
    print(f'Archived {len(files)} source files / {total} bytes / {len(packages)} dependency declarations / {revision}')


if __name__ == '__main__':
    main()
