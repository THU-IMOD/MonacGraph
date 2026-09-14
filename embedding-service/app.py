from __future__ import annotations

import os
from pathlib import Path


def _prepare_local_cache() -> None:
    """Keep Hugging Face weights and temp files inside the clone's .cache/."""
    root = Path(__file__).resolve().parents[1]
    cache = Path(os.environ.get("MONACGRAPH_CACHE", root / ".cache"))
    huggingface = cache / "huggingface"
    tmp = cache / "tmp"
    huggingface.mkdir(parents=True, exist_ok=True)
    tmp.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("HF_HOME", str(huggingface))
    os.environ.setdefault("HUGGINGFACE_HUB_CACHE", str(huggingface / "hub"))
    os.environ.setdefault("TRANSFORMERS_CACHE", str(huggingface))
    os.environ.setdefault("TMPDIR", str(tmp))
    os.environ.setdefault("TMP", str(tmp))
    os.environ.setdefault("TEMP", str(tmp))


_prepare_local_cache()

from functools import lru_cache

from fastapi import FastAPI
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer


MODEL_NAME = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-zh-v1.5")
MODEL_REVISION = os.getenv("EMBEDDING_REVISION", "main")
QUERY_PROMPT = os.getenv(
    "EMBEDDING_QUERY_PROMPT",
    "为这个句子生成表示以用于检索相关文章：",
)
NORMALIZED = os.getenv("EMBEDDING_NORMALIZED", "true").lower() == "true"

app = FastAPI(title="MonacGraph Embedding Service", version="1.0")


class EmbeddingRequest(BaseModel):
    texts: list[str] = Field(min_length=1)


class EmbeddingResponse(BaseModel):
    embeddings: list[list[float]]


@lru_cache(maxsize=1)
def model() -> SentenceTransformer:
    return SentenceTransformer(MODEL_NAME, revision=MODEL_REVISION)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/model-info")
def model_info() -> dict[str, object]:
    loaded = model()
    return {
        "model_name": MODEL_NAME,
        "model_revision": MODEL_REVISION,
        "dimension": loaded.get_sentence_embedding_dimension(),
        "normalized": NORMALIZED,
        "query_prompt": QUERY_PROMPT,
    }


@app.post("/embed/documents", response_model=EmbeddingResponse)
def embed_documents(request: EmbeddingRequest) -> EmbeddingResponse:
    vectors = model().encode(
        request.texts,
        normalize_embeddings=NORMALIZED,
        convert_to_numpy=True,
    )
    return EmbeddingResponse(embeddings=vectors.tolist())


@app.post("/embed/query", response_model=EmbeddingResponse)
def embed_query(request: EmbeddingRequest) -> EmbeddingResponse:
    texts = [QUERY_PROMPT + text for text in request.texts]
    vectors = model().encode(
        texts,
        normalize_embeddings=NORMALIZED,
        convert_to_numpy=True,
    )
    return EmbeddingResponse(embeddings=vectors.tolist())
