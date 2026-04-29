from __future__ import annotations

import json
import os
from typing import Any

from sqlalchemy import Column, String, Text, create_engine, Integer, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from services.storage.base import StorageBackend

Base = declarative_base()


class AccountModel(Base):
    """账号数据模型"""
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    access_token = Column(String(2048), unique=True, nullable=False, index=True)
    data = Column(Text,  nullable=False)


class AuthKeyModel(Base):
    """鉴权密钥数据模型"""
    __tablename__ = "auth_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_id = Column(String(255), unique=True, nullable=False, index=True)
    data = Column(Text, nullable=False)


class KVModel(Base):
    """通用 key-value 存储模型"""
    __tablename__ = "kv_store"

    key = Column(String(255), primary_key=True, nullable=False)
    value = Column(Text, nullable=False)


class DatabaseStorageBackend(StorageBackend):
    """数据库存储后端（支持 SQLite、PostgreSQL）"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        connect_args: dict[str, Any] = {}
        engine_url = database_url
        self._is_postgres = "postgresql" in database_url or "postgres" in database_url
        self._is_sqlite = "sqlite" in database_url

        if self._is_postgres:
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

        # Vercel Serverless 每个请求是独立进程，连接池没有意义且会导致
        # Neon 的 SSL EOF 错误（空闲连接被 Neon 断开后 pool 还持有引用）。
        # 在 Vercel 上使用 NullPool，每次请求建立新连接，用完立即关闭。
        is_vercel = bool(os.getenv("VERCEL"))
        if is_vercel and self._is_postgres:
            from sqlalchemy.pool import NullPool
            self.engine = create_engine(
                engine_url,
                connect_args=connect_args,
                poolclass=NullPool,
            )
        else:
            self.engine = create_engine(
                engine_url,
                connect_args=connect_args,
                pool_pre_ping=True,
                pool_recycle=300,    # Neon 空闲连接约 5 分钟断开，提前回收
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
            )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    # ── accounts ──────────────────────────────────────────────────────────────

    def load_accounts(self) -> list[dict[str, Any]]:
        session = self.Session()
        try:
            accounts = []
            for row in session.query(AccountModel).all():
                try:
                    data = json.loads(row.data)
                    if isinstance(data, dict):
                        accounts.append(data)
                except json.JSONDecodeError:
                    continue
            return accounts
        finally:
            session.close()

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """upsert 保存账号，防止重复入库，并删除已移除的账号。"""
        session = self.Session()
        try:
            valid: list[tuple[str, str]] = []
            for item in accounts:
                if not isinstance(item, dict):
                    continue
                token = str(item.get("access_token") or "").strip()
                if not token:
                    continue
                valid.append((token, json.dumps(item, ensure_ascii=False)))

            self._upsert(session, AccountModel, "access_token", valid)

            # 删除不在本次列表中的行
            if valid:
                session.query(AccountModel).filter(
                    AccountModel.access_token.notin_([t for t, _ in valid])
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
        session = self.Session()
        try:
            items = []
            for row in session.query(AuthKeyModel).all():
                try:
                    data = json.loads(row.data)
                    if isinstance(data, dict):
                        items.append(data)
                except json.JSONDecodeError:
                    continue
            return items
        finally:
            session.close()

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """upsert 保存鉴权密钥（含统计字段），并删除已移除的行。"""
        session = self.Session()
        try:
            valid: list[tuple[str, str]] = []
            for item in auth_keys:
                if not isinstance(item, dict):
                    continue
                # auth_key 的唯一标识字段名是 "id"（业务层），对应数据库列 key_id
                key_id = str(item.get("id") or "").strip()
                if not key_id:
                    continue
                valid.append((key_id, json.dumps(item, ensure_ascii=False)))

            self._upsert(session, AuthKeyModel, "key_id", valid)

            if valid:
                session.query(AuthKeyModel).filter(
                    AuthKeyModel.key_id.notin_([k for k, _ in valid])
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
        session = self.Session()
        try:
            serialized = json.dumps(value, ensure_ascii=False)
            if self._is_postgres:
                from sqlalchemy.dialects.postgresql import insert as pg_insert
                stmt = pg_insert(KVModel.__table__).values(key=key, value=serialized)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["key"],
                    set_={"value": stmt.excluded.value},
                )
                session.execute(stmt)
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

    # ── upsert core ───────────────────────────────────────────────────────────

    def _upsert(
        self,
        session: Any,
        model: type,
        unique_col: str,
        rows: list[tuple[str, str]],
    ) -> None:
        """统一 upsert 入口，按数据库类型分发。"""
        if not rows:
            return
        if self._is_postgres:
            self._upsert_postgres(session, model, unique_col, rows)
        elif self._is_sqlite:
            self._upsert_sqlite(session, model.__tablename__, unique_col, rows)
        else:
            self._upsert_generic(session, model, unique_col, rows)

    def _upsert_postgres(
        self,
        session: Any,
        model: type,
        unique_col: str,
        rows: list[tuple[str, str]],
    ) -> None:
        """PostgreSQL: INSERT ... ON CONFLICT DO UPDATE（SQLAlchemy Core，绕过 ORM 缓存）"""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        stmt = pg_insert(model.__table__).values(
            [{unique_col: k, "data": d} for k, d in rows]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[unique_col],
            set_={"data": stmt.excluded.data},
        )
        session.execute(stmt)

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

    # ── health / info ─────────────────────────────────────────────────────────

    def health_check(self) -> dict[str, Any]:
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
            return {"status": "unhealthy", "backend": "database", "error": str(e)}

    def get_backend_info(self) -> dict[str, Any]:
        if self._is_sqlite:
            db_type = "sqlite"
        elif self._is_postgres:
            db_type = "postgresql"
        elif "mysql" in self.database_url:
            db_type = "mysql"
        else:
            db_type = "unknown"
        return {
            "type": "database",
            "db_type": db_type,
            "description": f"数据库存储 ({db_type})",
            "database_url": self._mask_password(self.database_url),
        }

    @staticmethod
    def _mask_password(url: str) -> str:
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
