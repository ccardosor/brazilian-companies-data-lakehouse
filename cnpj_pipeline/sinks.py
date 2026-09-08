from __future__ import annotations

import os
import zipfile
from dataclasses import dataclass
from datetime import datetime
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
        return self.base_dir / month / "download-finalizado.txt"

    def is_done(self, month: str) -> bool:
        return self.done_path(month).exists()

    def has_zip(self, month: str, filename: str) -> bool:
        return self.zip_path(month, filename).exists()

    def ingest_zip(self, month: str, zip_path: Path, extract: bool = True) -> IngestedFile:
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
        done_path = self.done_path(month)
        done_path.parent.mkdir(parents=True, exist_ok=True)
        done_path.write_text(f"Finalizado em {datetime.now()}", encoding="utf-8")


class S3Sink:
    def __init__(
        self,
        bucket_name: str,
        prefix_root: str,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        client=None,
    ) -> None:
        self.bucket_name = bucket_name
        self.prefix_root = prefix_root.strip("/")
        self.client = client or self._build_client(region_name, endpoint_url)

    @staticmethod
    def _build_client(region_name: str | None, endpoint_url: str | None):
        import boto3

        return boto3.client(
            "s3",
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

    def zip_key(self, month: str, filename: str) -> str:
        return f"{self.prefix_root}/ano_mes={month}/zipped/{filename}"

    def extracted_prefix(self, month: str) -> str:
        return f"{self.prefix_root}/ano_mes={month}/unzipped/"

    def done_key(self, month: str) -> str:
        return f"{self.prefix_root}/ano_mes={month}/download-finalizado.txt"

    def is_done(self, month: str) -> bool:
        return self._object_exists(self.done_key(month))

    def has_zip(self, month: str, filename: str) -> bool:
        return self._object_exists(self.zip_key(month, filename))

    def ingest_zip(self, month: str, zip_path: Path, extract: bool = True) -> IngestedFile:
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
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=self.done_key(month),
            Body=f"Finalizado em {datetime.now()}".encode("utf-8"),
        )

    def check_access(self) -> None:
        self.client.head_bucket(Bucket=self.bucket_name)

    def _object_exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except Exception as error:
            response = getattr(error, "response", {})
            status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            error_code = response.get("Error", {}).get("Code")
            if status_code == 404 or error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
