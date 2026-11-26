"""Tests for multi-modal embeddings + cross-modal retrieval (issue #117).

Uses a deterministic CLIP-style stub provider (text + media share one vector
space) so no models/deps are needed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from lance_context.api import Context
from lance_context.embeddings import MultiModalEmbeddingProvider, supports_media


class StubCLIP:
    """Text and media land in the same 4-d space keyed by a concept label."""

    dims = 4

    def __init__(self) -> None:
        self.text_calls: list[str] = []
        self.media_calls: list[tuple[bytes, str]] = []

    @staticmethod
    def _vec(label: str) -> list[float]:
        return {
            "cat": [1.0, 0.0, 0.0, 0.0],
            "dog": [0.0, 1.0, 0.0, 0.0],
        }.get(label, [0.0, 0.0, 1.0, 0.0])

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.text_calls.extend(texts)
        return [
            self._vec("cat" if "cat" in t else "dog" if "dog" in t else "other")
            for t in texts
        ]

    def embed_media(self, items: list[tuple[bytes, str]]) -> list[list[float]]:
        self.media_calls.extend(items)
        out = []
        for data, _content_type in items:
            blob = bytes(data)
            label = "cat" if b"cat" in blob else "dog" if b"dog" in blob else "other"
            out.append(self._vec(label))
        return out


def _ctx(tmp_path: Path) -> Context:
    ctx = Context.create(str(tmp_path / "mm.lance"), embedding_dim=4)
    ctx._embedding_provider = StubCLIP()  # type: ignore[attr-defined]
    return ctx


def test_protocol_runtime_check() -> None:
    provider = StubCLIP()
    assert isinstance(provider, MultiModalEmbeddingProvider)
    assert supports_media(provider)

    class TextOnly:
        dims = 2

        def embed_texts(self, texts: list[str]) -> list[list[float]]:
            return [[0.0, 0.0] for _ in texts]

    assert not supports_media(TextOnly())


def test_image_auto_embedded_via_media(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    ctx.add("user", b"a cat image", content_type="image/png")

    assert ctx._embedding_provider.media_calls  # embed_media was used
    rec = ctx.list()[0]
    assert rec["embedding"][:2] == [1.0, 0.0]  # the shared-space "cat" vector


def test_cross_modal_text_query_retrieves_image(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    ctx.add("user", b"a cat photo", content_type="image/png", external_id="cat")
    ctx.add("user", b"a dog photo", content_type="image/png", external_id="dog")

    # Text query embeds via the shared text encoder -> retrieves the image.
    hits = ctx.search("a photo of a cat", limit=2)
    assert hits[0]["external_id"] == "cat"
    assert hits[0]["binary"] == b"a cat photo"


def test_batch_auto_embeds_images(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    ctx.add_many(
        [
            {
                "role": "user",
                "content": b"cat pic",
                "content_type": "image/png",
                "external_id": "c",
            },
            {
                "role": "user",
                "content": b"dog pic",
                "content_type": "image/png",
                "external_id": "d",
            },
        ]
    )
    by_ext = {r["external_id"]: r for r in ctx.list()}
    assert by_ext["c"]["embedding"][0] == 1.0
    assert by_ext["d"]["embedding"][1] == 1.0


def test_text_only_provider_does_not_embed_images(tmp_path: Path) -> None:
    ctx = Context.create(str(tmp_path / "t.lance"), embedding_dim=2)

    class TextOnly:
        dims = 2

        def embed_texts(self, texts: list[str]) -> list[list[float]]:
            return [[1.0, 0.0] for _ in texts]

    ctx._embedding_provider = TextOnly()  # type: ignore[attr-defined]
    ctx.add("user", b"some bytes")  # no embed_media -> not embedded
    assert ctx.list()[0]["embedding"] is None
