from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Column, String, Text, create_engine, Integer, text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from services.storage.base import StorageBackend

Base = declarative_base()


class AccountModel(Base):
    """账号数据模型"""
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    access_token = Column(String(2048), unique=True, nullable=False, index=True)
    data = Column(Text, nullable=False)  # JSON 格式存储完整账号数据


class AuthKeyModel(Base):
    """鉴权密钥数据模型"""
    __tablename__ = "auth_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_id = Column(String(255), unique=True, nullable=False, index=True)
    data = Column(Text, nullable=False)


class KVModel(Base):
    """通用 key-value 存储模型（用于持久化注册机配置等）"""
    __tablename__ = "kv_store"

    key = Column(String(255), primary_key=True, nullable=False)
    value = Column(Text, nullable=False)


class DatabaseStorageBackend(StorageBackend):
    """数据库存储后端（支持 SQLite、PostgreSQL、MySQL 等）"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        connect_args: dict[str, Any] = {}
        engine_url = database_url
        self._is_postgres = "postgresql" in database_url or "postgres" in database_url
        self._is_sqlite = "sqlite" in database_url

        if self._is_postgres:
            # Try psycopg2 first; if unavailable, rewrite URL to use pg8000
            try:
                import psycopg2  # noqa: F401
            except ImportError:
                engine_url = database_url.replace("postgresql://", "postgresql+pg8000://", 1)
                engine_url = engine_url.replace("postgres://", "postgresql+pg8000://", 1)

            if "pg8000" in engine_url:
                import ssl as _ssl
                ssl_ctx = _ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = _ssl.CERT_NONE
                connect_args["ssl_context"] = ssl_ctx
                engine_url = engine_url.split("?")[0]

        self.engine = create_engine(
            engine_url,
            connect_args=connect_args,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    # ── accounts ──────────────────────────────────────────────────────────────

    def load_accounts(self) -> list[dict[str, Any]]:
        """从数据库加载账号数据"""
        session = self.Session()
        try:
            accounts = []
            for row in session.query(AccountModel).all():
                try:
                    account_data = json.loads(row.data)
                    if isinstance(account_data, dict):
                        accounts.append(account_data)
                except json.JSONDecodeError:
                    continue
            return accounts
        finally:
            session.close()

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """使用 upsert 保存账号数据，防止重复入库。
        - PostgreSQL: INSERT ... ON CONFLICT (access_token) DO UPDATE SET data = EXCLUDED.data
        - SQLite:     INSERT OR REPLACE INTO accounts (access_token, data) VALUES (...)
        - 其他数据库: 先查后插/更新（兼容回退）
        同时删除不在本次列表中的账号（保持与内存状态一致）。
        """
        session = self.Session()
        try:
            # 收集本次有效的 token 集合
            valid_tokens: list[tuple[str, str]] = []
            for item in accounts:
                if not isinstance(item, dict):
                    continue
                token = str(item.get("access_token") or "").strip()
                if not token:
                    continue
                valid_tokens.append((token, json.dumps(item, ensure_ascii=False)))

            if self._is_postgres:
                self._upsert_postgres(session, AccountModel, "access_token", valid_tokens)
            elif self._is_sqlite:
                self._upsert_sqlite(session, "accounts", "access_token", valid_tokens)
            else:
                self._upsert_generic(session, AccountModel, "access_token", valid_tokens)

            # 删除已不在列表中的账号
            if valid_tokens:
                token_set = [t for t, _ in valid_tokens]
                session.query(AccountModel).filter(
                    AccountModel.access_token.notin_(token_set)
                ).delete(synchronize_session=False)
            else:
                session.query(AccountModel).delete()

            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    # ── auth_keys ─────────────────────────────────────────────────────────────

    def load_auth_keys(self) -> list[dict[str, Any]]:
        """从数据库加载鉴权密钥数据"""
        return self._load_rows(AuthKeyModel)

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存鉴权密钥数据到数据库（upsert）"""
        session = self.Session()
        try:
            valid_rows: list[tuple[str, str]] = []
            for item in auth_keys:
                if not isinstance(item, dict):
                    continue
                key_id = str(item.get("id") or "").strip()
                if not key_id:
                    continue
                valid_rows.append((key_id, json.dumps(item, ensure_ascii=False)))

            if self._is_postgres:
                self._upsert_postgres(session, AuthKeyModel, "key_id", valid_rows)
            elif self._is_sqlite:
                self._upsert_sqlite(session, "auth_keys", "key_id", valid_rows)
            else:
                self._upsert_generic(session, AuthKeyModel, "key_id", valid_rows)

            if valid_rows:
                key_set = [k for k, _ in valid_rows]
                session.query(AuthKeyModel).filter(
                    AuthKeyModel.key_id.notin_(key_set)
                ).delete(synchronize_session=False)
            else:
                session.query(AuthKeyModel).delete()

            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    # ── kv_store ──────────────────────────────────────────────────────────────

    def load_kv(self, key: str) -> dict[str, Any] | None:
        """从数据库加载 key-value 记录"""
        session = self.Session()
        try:
            row = session.query(KVModel).filter(KVModel.key == key).first()
            if row is None:
                return None
            return json.loads(row.value)
        except Exception:
            return None
        finally:
            session.close()

    def save_kv(self, key: str, value: dict[str, Any]) -> None:
        """保存 key-value 记录到数据库（upsert）"""
        session = self.Session()
        try:
            serialized = json.dumps(value, ensure_ascii=False)
            if self._is_postgres:
                session.execute(
                    text(
                        "INSERT INTO kv_store (key, value) VALUES (:key, :value) "
                        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
                    ),
                    {"key": key, "value": serialized},
                )
            elif self._is_sqlite:
                session.execute(
                    text("INSERT OR REPLACE INTO kv_store (key, value) VALUES (:key, :value)"),
                    {"key": key, "value": serialized},
                )
            else:
                row = session.query(KVModel).filter(KVModel.key == key).first()
                if row is None:
                    session.add(KVModel(key=key, value=serialized))
                else:
                    row.value = serialized
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    # ── upsert helpers ────────────────────────────────────────────────────────

    def _upsert_postgres(
        self,
        session: Any,
        model: type,
        unique_col: str,
        rows: list[tuple[str, str]],
    ) -> None:
        """PostgreSQL: INSERT ... ON CONFLICT DO UPDATE"""
        table = model.__tablename__
        for key_value, data_value in rows:
            session.execute(
                text(
                    f"INSERT INTO {table} ({unique_col}, data) VALUES (:{unique_col}, :data) "
                    f"ON CONFLICT ({unique_col}) DO UPDATE SET data = EXCLUDED.data"
                ),
                {unique_col: key_value, "data": data_value},
            )

    def _upsert_sqlite(
        self,
        session: Any,
        table: str,
        unique_col: str,
        rows: list[tuple[str, str]],
    ) -> None:
        """SQLite: INSERT OR REPLACE"""
        for key_value, data_value in rows:
            session.execute(
                text(f"INSERT OR REPLACE INTO {table} ({unique_col}, data) VALUES (:{unique_col}, :data)"),
                {unique_col: key_value, "data": data_value},
            )

    def _upsert_generic(
        self,
        session: Any,
        model: type,
        unique_col: str,
        rows: list[tuple[str, str]],
    ) -> None:
        """其他数据库：先查后插/更新"""
        for key_value, data_value in rows:
            existing = session.query(model).filter(
                getattr(model, unique_col) == key_value
            ).first()
            if existing is None:
                session.add(model(**{unique_col: key_value, "data": data_value}))
            else:
                existing.data = data_value

    # ── misc ──────────────────────────────────────────────────────────────────

    def _load_rows(self, model: type) -> list[dict[str, Any]]:
        session = self.Session()
        try:
            items = []
            for row in session.query(model).all():
                try:
                    item_data = json.loads(row.data)
                    if isinstance(item_data, dict):
                        items.append(item_data)
                except json.JSONDecodeError:
                    continue
            return items
        finally:
            session.close()

    def health_check(self) -> dict[str, Any]:
        """健康检查"""
        try:
            session = self.Session()
            try:
                session.execute(text("SELECT 1"))
                count = session.query(AccountModel).count()
                auth_key_count = session.query(AuthKeyModel).count()
                return {
                    "status": "healthy",
                    "backend": "database",
                    "database_url": self._mask_password(self.database_url),
                    "account_count": count,
                    "auth_key_count": auth_key_count,
                }
            finally:
                session.close()
        except Exception as e:
            return {
                "status": "unhealthy",
                "backend": "database",
                "error": str(e),
            }

    def get_backend_info(self) -> dict[str, Any]:
        """获取存储后端信息"""
        db_type = "unknown"
        if self._is_sqlite:
            db_type = "sqlite"
        elif self._is_postgres:
            db_type = "postgresql"
        elif "mysql" in self.database_url:
            db_type = "mysql"

        return {
            "type": "database",
            "db_type": db_type,
            "description": f"数据库存储 ({db_type})",
            "database_url": self._mask_password(self.database_url),
        }

    @staticmethod
    def _mask_password(url: str) -> str:
        """隐藏数据库连接字符串中的密码"""
        if "://" not in url:
            return url
        try:
            protocol, rest = url.split("://", 1)
            if "@" in rest:
                credentials, host = rest.split("@", 1)
                if ":" in credentials:
                    username, _ = credentials.split(":", 1)
                    return f"{protocol}://{username}:****@{host}"
            return url
        except Exception:
            return url



class DatabaseStorageBackend(StorageBackend):
    """数据库存储后端（支持 SQLite、PostgreSQL、MySQL 等）"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        # Neon / Vercel: psycopg2-binary may not be available in serverless environments.
        # Automatically fall back to pg8000 (pure-Python, no compilation needed).
        # Also handle SSL requirements for Neon connections.
        connect_args: dict[str, Any] = {}
        engine_url = database_url

        if "postgresql" in database_url or "postgres" in database_url:
            # Try psycopg2 first; if unavailable, rewrite URL to use pg8000
            try:
                import psycopg2  # noqa: F401
            except ImportError:
                # Replace driver with pg8000
                engine_url = database_url.replace("postgresql://", "postgresql+pg8000://", 1)
                engine_url = engine_url.replace("postgres://", "postgresql+pg8000://", 1)

            # Neon requires SSL — pass ssl=True via connect_args for pg8000,
            # or rely on the ?sslmode=require query param for psycopg2.
            if "pg8000" in engine_url:
                import ssl as _ssl
                ssl_ctx = _ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = _ssl.CERT_NONE
                connect_args["ssl_context"] = ssl_ctx
                # pg8000 doesn't understand sslmode/channel_binding query params — strip them
                engine_url = engine_url.split("?")[0]

        self.engine = create_engine(
            engine_url,
            connect_args=connect_args,
            pool_pre_ping=True,  # 自动检测连接是否有效
            pool_recycle=3600,   # 1小时回收连接
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def load_accounts(self) -> list[dict[str, Any]]:
        """从数据库加载账号数据"""
        session = self.Session()
        try:
            accounts = []
            for row in session.query(AccountModel).all():
                try:
                    account_data = json.loads(row.data)
                    if isinstance(account_data, dict):
                        accounts.append(account_data)
                except json.JSONDecodeError:
                    continue
            return accounts
        finally:
            session.close()

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """保存账号数据到数据库"""
        self._save_rows(AccountModel, accounts, "access_token")

    def load_auth_keys(self) -> list[dict[str, Any]]:
        """从数据库加载鉴权密钥数据"""
        return self._load_rows(AuthKeyModel)

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存鉴权密钥数据到数据库"""
        self._save_rows(AuthKeyModel, auth_keys, "id", "key_id")

    def _load_rows(self, model: type[AccountModel] | type[AuthKeyModel]) -> list[dict[str, Any]]:
        session = self.Session()
        try:
            items = []
            for row in session.query(model).all():
                try:
                    item_data = json.loads(row.data)
                    if isinstance(item_data, dict):
                        items.append(item_data)
                except json.JSONDecodeError:
                    continue
            return items
        finally:
            session.close()

    def _save_rows(
        self,
        model: type[AccountModel] | type[AuthKeyModel],
        items: list[dict[str, Any]],
        source_key: str,
        target_key: str | None = None,
    ) -> None:
        session = self.Session()
        try:
            session.query(model).delete()
            for item in items:
                if not isinstance(item, dict):
                    continue
                key_value = str(item.get(source_key) or "").strip()
                if not key_value:
                    continue
                session.add(
                    model(
                        **{target_key or source_key: key_value},
                        data=json.dumps(item, ensure_ascii=False),
                    )
                )
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def load_kv(self, key: str) -> dict[str, Any] | None:
        """从数据库加载 key-value 记录"""
        session = self.Session()
        try:
            row = session.query(KVModel).filter(KVModel.key == key).first()
            if row is None:
                return None
            return json.loads(row.value)
        except Exception:
            return None
        finally:
            session.close()

    def save_kv(self, key: str, value: dict[str, Any]) -> None:
        """保存 key-value 记录到数据库（upsert）"""
        session = self.Session()
        try:
            row = session.query(KVModel).filter(KVModel.key == key).first()
            serialized = json.dumps(value, ensure_ascii=False)
            if row is None:
                session.add(KVModel(key=key, value=serialized))
            else:
                row.value = serialized
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def health_check(self) -> dict[str, Any]:
        """健康检查"""
        try:
            session = self.Session()
            try:
                # 尝试执行简单查询
                session.execute(text("SELECT 1"))
                count = session.query(AccountModel).count()
                auth_key_count = session.query(AuthKeyModel).count()
                return {
                    "status": "healthy",
                    "backend": "database",
                    "database_url": self._mask_password(self.database_url),
                    "account_count": count,
                    "auth_key_count": auth_key_count,
                }
            finally:
                session.close()
        except Exception as e:
            return {
                "status": "unhealthy",
                "backend": "database",
                "error": str(e),
            }

    def get_backend_info(self) -> dict[str, Any]:
        """获取存储后端信息"""
        db_type = "unknown"
        if "sqlite" in self.database_url:
            db_type = "sqlite"
        elif "postgresql" in self.database_url or "postgres" in self.database_url:
            db_type = "postgresql"
        elif "mysql" in self.database_url:
            db_type = "mysql"
        
        return {
            "type": "database",
            "db_type": db_type,
            "description": f"数据库存储 ({db_type})",
            "database_url": self._mask_password(self.database_url),
        }

    @staticmethod
    def _mask_password(url: str) -> str:
        """隐藏数据库连接字符串中的密码"""
        if "://" not in url:
            return url
        try:
            protocol, rest = url.split("://", 1)
            if "@" in rest:
                credentials, host = rest.split("@", 1)
                if ":" in credentials:
                    username, _ = credentials.split(":", 1)
                    return f"{protocol}://{username}:****@{host}"
            return url
        except Exception:
            return url
