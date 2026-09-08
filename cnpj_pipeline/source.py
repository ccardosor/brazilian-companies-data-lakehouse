from __future__ import annotations

import xml.etree.ElementTree as ET
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin


@dataclass(frozen=True)
class RemoteItem:
    name: str
    is_dir: bool
    href: str
    size_bytes: int | None = None


class ReceitaCnpjSource:
    """Lista e faz o download dos dados de CNPJ do endpoint WebDAV Público."""

    def __init__(self, base_url: str, timeout: int = 60, session=None) -> None:
        self.base_url = base_url if base_url.endswith("/") else f"{base_url}/"
        self.timeout = timeout
        self.session = session or self._build_session()

    @staticmethod
    def _build_session():
        import requests

        return requests

    def list_directory(self, path: str = "") -> list[RemoteItem]:
        url = urljoin(self.base_url, path)
        headers = {"Depth": "1", "Content-Type": "application/xml"}
        body = """<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:resourcetype/>
    <d:getcontentlength/>
  </d:prop>
</d:propfind>"""

        response = self.session.request(
            "PROPFIND", url, headers=headers, data=body, timeout=self.timeout
        )
        response.raise_for_status()

        namespace = {"d": "DAV:"}
        root = ET.fromstring(response.content)
        items: list[RemoteItem] = []

        for response_element in root.findall("d:response", namespace):
            href_element = response_element.find("d:href", namespace)
            if href_element is None or href_element.text is None:
                continue

            href = href_element.text
            name = href.rstrip("/").split("/")[-1]

            resource_type = response_element.find(
                "d:propstat/d:prop/d:resourcetype", namespace
            )
            is_dir = (
                resource_type is not None
                and resource_type.find("d:collection", namespace) is not None
            )

            size_text = response_element.findtext(
                "d:propstat/d:prop/d:getcontentlength", namespaces=namespace
            )
            size_bytes = int(size_text) if size_text and size_text.isdigit() else None

            items.append(
                RemoteItem(name=name, is_dir=is_dir, href=href, size_bytes=size_bytes)
            )

        return items

    def list_months(self) -> list[str]:
        months = [
            item.name
            for item in self.list_directory()
            if item.is_dir and re.match(r"^\d{4}-\d{2}$", item.name)
        ]
        return sorted(months)

    def latest_month(self) -> str:
        months = self.list_months()
        if not months:
            raise RuntimeError("Nenhum diretorio YYYY-MM foi encontrado na origem.")
        return months[-1]

    def list_zip_files(self, month: str) -> list[RemoteItem]:
        return sorted(
            [
                item
                for item in self.list_directory(f"{month}/")
                if not item.is_dir and item.name.lower().endswith(".zip")
            ],
            key=lambda item: item.name,
        )

    def download_file(self, month: str, filename: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_destination = destination.with_suffix(f"{destination.suffix}.part")
        url = urljoin(self.base_url, f"{month}/{filename}")

        with self.session.get(url, stream=True, timeout=self.timeout) as response:
            response.raise_for_status()
            with temp_destination.open("wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        file.write(chunk)

        temp_destination.replace(destination)
