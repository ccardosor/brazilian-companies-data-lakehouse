from __future__ import annotations

import json
import os
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True)
class IngestedFile:
    zip_location: str
    extracted_locations: list[str]


class LocalSink:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir

    def zip_path(self, month: str, filename: str) -> Path:
        return self.base_dir / month / filename

    def done_path(self, month: str) -> Path:
        return self.manifest_path(month)

    def manifest_path(self, month: str) -> Path:
        return self.base_dir / month / "ingestao-manifest.json"

    def legacy_done_path(self, month: str) -> Path:
        return self.base_dir / month / "download-finalizado.txt"

    def is_done(self, month: str) -> bool:
        return (
            self.manifest_path(month).exists()
            or self.legacy_done_path(month).exists()
        )

    def has_zip(self, month: str, filename: str) -> bool:
        return self.zip_path(month, filename).exists()

    def ingest_zip(
        self, month: str, zip_path: Path, extract: bool = True
    ) -> IngestedFile:
        extracted_locations: list[str] = []
        if extract:
            extract_dir = self.base_dir / f"{month}_unzipped"
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, "r") as archive:
                archive.extractall(extract_dir)
                extracted_locations = [
                    str(extract_dir / name) for name in archive.namelist()
                ]

        return IngestedFile(
            zip_location=str(zip_path),
            extracted_locations=extracted_locations,
        )

    def mark_done(self, month: str) -> None:
        manifest_path = self.manifest_path(month)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ano_mes": month,
            "destino": "local",
            "finalizado_em_utc": datetime.now(UTC).isoformat(),
        }
        manifest_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


class S3Sink:
    def __init__(
        self,
        bucket_name: str,
        prefix_root: str,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        url_style: str | None = None,
        use_ssl: bool | None = None,
        client=None,
    ) -> None:
        self.bucket_name = bucket_name
        self.prefix_root = prefix_root.strip("/")
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.url_style = url_style or ("path" if endpoint_url else "vhost")
        self.use_ssl = use_ssl if use_ssl is not None else _ssl_padrao(endpoint_url)
        self.client = client or self._build_client(
            region_name, endpoint_url, self.url_style
        )

    @staticmethod
    def _build_client(
        region_name: str | None,
        endpoint_url: str | None,
        url_style: str = "vhost",
    ):
        import boto3
        from botocore.config import Config

        # MinIO e outros compativeis exigem path-style nas requisicoes.
        addressing_style = "path" if url_style == "path" else "virtual"

        return boto3.client(
            "s3",
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=Config(s3={"addressing_style": addressing_style}),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

    def zip_key(self, month: str, filename: str) -> str:
        return f"{self.prefix_root}/ano_mes={month}/zipped/{filename}"

    def extracted_prefix(self, month: str) -> str:
        return f"{self.prefix_root}/ano_mes={month}/unzipped/"

    def done_key(self, month: str) -> str:
        return self.manifest_key(month)

    def manifest_key(self, month: str) -> str:
        return f"{self.prefix_root}/ano_mes={month}/ingestao-manifest.json"

    def legacy_done_key(self, month: str) -> str:
        return f"{self.prefix_root}/ano_mes={month}/download-finalizado.txt"

    def is_done(self, month: str) -> bool:
        return self._object_exists(self.manifest_key(month)) or self._object_exists(
            self.legacy_done_key(month)
        )

    def has_zip(self, month: str, filename: str) -> bool:
        return self._object_exists(self.zip_key(month, filename))

    def ingest_zip(
        self, month: str, zip_path: Path, extract: bool = True
    ) -> IngestedFile:
        zip_key = self.zip_key(month, zip_path.name)
        self.client.upload_file(str(zip_path), self.bucket_name, zip_key)

        extracted_locations: list[str] = []
        if extract:
            with zipfile.ZipFile(zip_path, "r") as archive:
                for inner_name in archive.namelist():
                    key = f"{self.extracted_prefix(month)}{inner_name}"
                    with archive.open(inner_name) as file:
                        self.client.upload_fileobj(file, self.bucket_name, key)
                    extracted_locations.append(f"s3://{self.bucket_name}/{key}")

        return IngestedFile(
            zip_location=f"s3://{self.bucket_name}/{zip_key}",
            extracted_locations=extracted_locations,
        )

    def mark_done(self, month: str) -> None:
        payload = {
            "ano_mes": month,
            "destino": "s3",
            "bucket": self.bucket_name,
            "prefixo": f"{self.prefix_root}/ano_mes={month}/",
            "finalizado_em_utc": datetime.now(UTC).isoformat(),
        }
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=self.manifest_key(month),
            Body=(json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode(
                "utf-8"
            ),
        )

    def check_access(self) -> None:
        self.client.head_bucket(Bucket=self.bucket_name)

    def zipped_prefix(self, month: str) -> str:
        return f"{self.prefix_root}/ano_mes={month}/zipped/"

    def s3_uri(self, key: str) -> str:
        return f"s3://{self.bucket_name}/{key}"

    def object_exists(self, key: str) -> bool:
        return self._object_exists(key)

    def object_size(self, key: str) -> int | None:
        try:
            resposta = self.client.head_object(Bucket=self.bucket_name, Key=key)
        except Exception as error:
            if _erro_de_objeto_ausente(error):
                return None
            raise

        if isinstance(resposta, dict):
            return resposta.get("ContentLength")
        return None

    def list_keys(self, prefix: str) -> list[str]:
        """Lista todas as chaves sob um prefixo, seguindo a paginacao do S3."""
        chaves: list[str] = []
        token: str | None = None

        while True:
            parametros: dict[str, object] = {
                "Bucket": self.bucket_name,
                "Prefix": prefix,
            }
            if token:
                parametros["ContinuationToken"] = token

            resposta = self.client.list_objects_v2(**parametros) or {}
            for item in resposta.get("Contents", []) or []:
                chave = item.get("Key")
                if chave and not chave.endswith("/"):
                    chaves.append(chave)

            if not resposta.get("IsTruncated"):
                break

            token = resposta.get("NextContinuationToken")
            if not token:
                break

        return sorted(chaves)

    def get_bytes(self, key: str) -> bytes | None:
        try:
            resposta = self.client.get_object(Bucket=self.bucket_name, Key=key)
        except Exception as error:
            if _erro_de_objeto_ausente(error):
                return None
            raise

        corpo = resposta["Body"]
        return corpo.read()

    def put_bytes(self, key: str, body: bytes) -> None:
        self.client.put_object(Bucket=self.bucket_name, Key=key, Body=body)

    def download_to(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket_name, key, str(destination))

    def upload_from(self, source: Path, key: str) -> None:
        self.client.upload_file(str(source), self.bucket_name, key)

    def duckdb_config(self) -> dict[str, object]:
        """Configuracoes do httpfs do DuckDB equivalentes a este sink."""
        configuracao: dict[str, object] = {
            "s3_url_style": self.url_style,
            "s3_use_ssl": self.use_ssl,
        }
        if self.region_name:
            configuracao["s3_region"] = self.region_name
        if self.endpoint_url:
            configuracao["s3_endpoint"] = _endpoint_sem_esquema(self.endpoint_url)

        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        session_token = os.getenv("AWS_SESSION_TOKEN")
        if access_key:
            configuracao["s3_access_key_id"] = access_key
        if secret_key:
            configuracao["s3_secret_access_key"] = secret_key
        if session_token:
            configuracao["s3_session_token"] = session_token

        return configuracao

    def _object_exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except Exception as error:
            if _erro_de_objeto_ausente(error):
                return False
            raise


def _erro_de_objeto_ausente(error: Exception) -> bool:
    response = getattr(error, "response", {})
    status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    error_code = response.get("Error", {}).get("Code")
    return status_code == 404 or error_code in {"404", "NoSuchKey", "NotFound"}


def _ssl_padrao(endpoint_url: str | None) -> bool:
    if not endpoint_url:
        return True
    return not endpoint_url.lower().startswith("http://")


def _endpoint_sem_esquema(endpoint_url: str) -> str:
    """O httpfs do DuckDB espera apenas host[:porta], sem esquema nem barra final."""
    sem_esquema = endpoint_url.split("://", 1)[-1]
    return sem_esquema.rstrip("/")
