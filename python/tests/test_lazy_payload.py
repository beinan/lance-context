"""Tests for lazy / projected payload reads (issue #116)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from lance_context.api import Context

IMG = b"\x89PNG\r\n\x1a\n fake image bytes"


def test_list_can_exclude_binary_and_fetch_on_demand(tmp_path: Path) -> None:
    ctx = Context.create(str(tmp_path / "c.lance"), embedding_dim=4)
    ctx.add(
        "user",
        IMG,
        content_type="image/png",
        embedding=[1.0, 0.0, 0.0, 0.0],
    )

    # Default read includes the bytes.
    full = ctx.list()
    assert len(full) == 1
    assert full[0]["binary"] == IMG
    assert full[0]["content_type"] == "image/png"
    rec_id = full[0]["id"]

    # Excluding binary returns metadata without the bytes.
    lean = ctx.list(include_binary=False)
    assert lean[0]["binary"] is None
    assert lean[0]["content_type"] == "image/png"
    assert lean[0]["id"] == rec_id
    assert lean[0]["embedding"] is not None  # embedding still present

    # metadata-only: drop embedding too.
    meta = ctx.list(include_binary=False, include_embedding=False)
    assert meta[0]["binary"] is None
    assert meta[0]["embedding"] is None

    # Fetch the bytes on demand.
    assert ctx.get_blob(rec_id) == IMG
    assert ctx.get_blob("does-not-exist") is None


def test_search_can_exclude_binary(tmp_path: Path) -> None:
    ctx = Context.create(str(tmp_path / "c.lance"), embedding_dim=4)
    ctx.add("user", b"img-a", content_type="image/png", embedding=[1.0, 0.0, 0.0, 0.0])
    ctx.add("user", b"img-b", content_type="image/png", embedding=[0.0, 1.0, 0.0, 0.0])

    hits = ctx.search([1.0, 0.0, 0.0, 0.0], include_binary=False)
    assert len(hits) == 2
    assert all(h["binary"] is None for h in hits)

    # Default search still returns the bytes.
    full = ctx.search([1.0, 0.0, 0.0, 0.0])
    assert any(h["binary"] is not None for h in full)
