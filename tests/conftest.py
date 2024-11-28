# tests/conftest.py
"""Test fixtures for Torno"""

import pytest
from datetime import datetime
from torno.core import Schema, EnrichmentVersion, EnrichmentDefinition, FeatureStore

@pytest.fixture
def basic_schema():
    return Schema(
        fields={"text": "str", "length": "int"},
        required=["text"],
        validators={
            "length": lambda x: x > 0 if x is not None else True
        }
    )

@pytest.fixture
def sample_version(basic_schema):
    return EnrichmentVersion.create(
        prompt="Extract key points from {text}",
        model="gpt-4",
        params={"temperature": 0.7, "max_tokens": 150},
        input_schema=basic_schema,
        output_schema=Schema(
            fields={"key_points": "list", "summary": "str"},
            required=["key_points"]
        )
    )

@pytest.fixture
def sample_enrichment(sample_version):
    enrichment = EnrichmentDefinition(
        name="key_points_extractor",
        description="Extracts key points from text"
    )
    enrichment.add_version(sample_version)
    return enrichment

@pytest.fixture
def feature_store():
    return FeatureStore()