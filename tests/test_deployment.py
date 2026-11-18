import os
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

import persist_db


ROOT = Path(__file__).resolve().parents[1]


class DeploymentTests(unittest.TestCase):
    def run_app(self, script, **settings):
        # Configuration and module state are read at import time. Keep each boot
        # separate and never pick up the operator's database or Telegram settings.
        with tempfile.TemporaryDirectory() as data_dir:
            env = {key: os.environ[key] for key in ("PATH", "PYTHONPATH", "SYSTEMROOT") if key in os.environ}
            env.update(
                HOME=data_dir,
                DATA_DIR=data_dir,
                PYTHONDONTWRITEBYTECODE="1",
                VW_HTTP_ONLY="1",
                KEEPALIVE_ENABLED="0",
                ADMIN_USERNAME="test-owner",
                ADMIN_PASSWORD="fixture-owner-password",
                SECRET_KEY="fixture-disposable-key",
                PUBLIC_BASE_URL="https://panel.example.test",
                PORT="38123",
            )
            env.update(settings)
            result = subprocess.run(
                [sys.executable, "-c", textwrap.dedent(script)],
                cwd=ROOT, env=env, capture_output=True, text=True, timeout=30,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_http_only_launcher_uses_the_assigned_port(self):
        self.run_app('''
            import runpy
            from unittest.mock import patch

            with patch('uvicorn.Server') as server, patch('tg_proxy.attach_mux') as mux:
                runpy.run_path('app.py', run_name='__main__')
            config = server.call_args.args[0]
            assert (config.host, config.port, config.workers) == ('0.0.0.0', 38123, 1)
            server.return_value.run.assert_called_once()
            mux.assert_not_called()
        ''')

    def test_environment_credentials_win_with_restored_state(self):
        self.run_app('''
            import asyncio
            import json
            import os
            from unittest.mock import patch

            import httpx
            from app import app
            from main import DATA_FILE, hash_password

            DATA_FILE.write_text(json.dumps({
                'username': 'old-owner',
                'password_hash': hash_password('fixture-old-password'),
            }))

            async def check():
                with patch('tcp_relay.start_tcp_relay') as relay:
                    async with app.router.lifespan_context(app), httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url='https://panel.example.test',
                    ) as client:
                        health = await client.get('/healthz')
                        assert (health.status_code, health.text) == (200, 'pong')
                        assert (await client.get('/api/links')).status_code == 401
                        login = await client.post('/api/login', json={
                            'username': os.environ['ADMIN_USERNAME'], 'password': os.environ['ADMIN_PASSWORD'],
                        })
                        assert login.status_code == 200, login.text
                        assert 'Secure' in login.headers['Set-Cookie']
                        assert (await client.get('/dashboard')).status_code == 200
                        assert (await client.get('/api/tgproxy')).status_code == 409
                        assert not hasattr(app.state, 'tg_proxy')
                        for field in ('password', 'username'):
                            response = await client.post(f'/api/change-{field}', json={})
                            assert response.status_code == 409
                        assert (await client.post('/api/logout')).status_code == 200
                        assert (await client.get('/api/links')).status_code == 401
                    relay.assert_not_called()

            asyncio.run(check())
        ''')

    def test_missing_password_stops_a_fresh_install(self):
        self.run_app('''
            import asyncio
            from app import app
            from main import DATA_FILE

            async def check():
                try:
                    async with app.router.lifespan_context(app):
                        raise AssertionError('Startup should fail without a configured owner password')
                except RuntimeError as exc:
                    assert 'ADMIN_PASSWORD' in str(exc), str(exc)
                assert not DATA_FILE.exists()

            asyncio.run(check())
        ''', ADMIN_PASSWORD="")

    def test_existing_owner_is_preserved_without_environment_override(self):
        self.run_app('''
            import asyncio
            import json
            from app import app
            from main import DATA_FILE, hash_password, verify_admin_credentials

            DATA_FILE.write_text(json.dumps({
                'username': 'saved-owner', 'password_hash': hash_password('fixture-saved-password'),
            }))

            async def check():
                async with app.router.lifespan_context(app):
                    assert verify_admin_credentials('saved-owner', 'fixture-saved-password')[0]

            asyncio.run(check())
        ''', ADMIN_USERNAME="", ADMIN_PASSWORD="")

    def test_deleting_last_inbound_does_not_expand_admin_access(self):
        self.run_app('''
            import asyncio
            import os
            import httpx
            from app import app

            async def check():
                async with app.router.lifespan_context(app), httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url='https://panel.example.test',
                ) as owner, httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url='https://panel.example.test',
                ) as limited:
                    login = await owner.post('/api/login', json={
                        'username': os.environ['ADMIN_USERNAME'], 'password': os.environ['ADMIN_PASSWORD'],
                    })
                    assert login.status_code == 200
                    create = await owner.post('/api/links', json={'label': 'Fixture', 'protocol': 'vless-ws'})
                    assert create.status_code == 200, create.text
                    uid = create.json()['uuid']
                    credentials = {'username': 'limited-fixture', 'password': 'fixture-limited-password'}
                    create = await owner.post('/api/admins', json={**credentials, 'allowed_inbounds': [uid]})
                    assert create.status_code == 200, create.text
                    aid = create.json()['admin']['id']
                    assert (await limited.post('/api/login', json=credentials)).status_code == 200
                    assert [link['uuid'] for link in (await limited.get('/api/links')).json()['links']] == [uid]
                    assert (await owner.delete(f'/api/links/{uid}')).status_code == 200
                    assert (await owner.get('/api/links')).json()['links']  # unrelated inbound still exists
                    assert (await limited.get('/api/links')).json()['links'] == []
                    assert (await limited.get('/api/me')).json()['admin']['scoped'] is True
                    # An edit without an explicit unlock must preserve the empty restriction.
                    edited = await owner.patch(f'/api/admins/{aid}', json={'allowed_inbounds': []})
                    assert edited.status_code == 200 and edited.json()['admin']['scoped'] is True

            asyncio.run(check())
        ''')

    def test_failed_database_restore_stops_before_local_setup(self):
        self.run_app('''
            import os
            from pathlib import Path
            from unittest.mock import patch

            with patch('persist_db.restore_sync', return_value=False):
                try:
                    import app
                except RuntimeError as exc:
                    assert 'PostgreSQL restore failed' in str(exc), str(exc)
                else:
                    raise AssertionError('Startup should fail rather than run without the configured database')
            assert not list(Path(os.environ['DATA_DIR']).iterdir())
        ''', DATABASE_URL="postgresql://fixture@database.invalid/vodi", SECRET_KEY="")


class DatabaseTLSTests(unittest.TestCase):
    def test_require_does_not_retry_without_certificate_verification(self):
        url = "postgresql://fixture@database.invalid/vodi?sslmode=require"
        with patch.object(persist_db, "DB_URL", url), patch(
            "pg8000.native.Connection", side_effect=ssl.SSLCertVerificationError,
        ) as connect:
            with self.assertRaises(ssl.SSLCertVerificationError):
                persist_db._connect()
            connect.assert_called_once()
            context = connect.call_args.kwargs["ssl_context"]
            self.assertTrue(context.check_hostname)
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)

    def test_disable_is_explicit_for_local_databases(self):
        url = "postgresql://fixture@localhost/vodi?sslmode=disable"
        with patch.object(persist_db, "DB_URL", url), patch("pg8000.native.Connection") as connect:
            persist_db._connect()
            self.assertIs(connect.call_args.kwargs["ssl_context"], False)


if __name__ == "__main__":
    unittest.main()
