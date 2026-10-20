"""Shared test helpers (importable as ``tests.helpers``)."""

from __future__ import annotations

from dataclasses import replace

from uk_flood.settings import Settings


def with_base_url(settings: Settings, url: str) -> Settings:
    return replace(settings, ea_api_base_url=url)
