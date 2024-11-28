# tests/test_core.py
"""Tests for core Torno functionality"""

from datetime import datetime

import pytest

from torno.core import (
    EnrichmentDefinition,
    EnrichmentStatus,
    EnrichmentVersion,
    EnrichmentVersionConfig,
)


class TestSchema:
    def test_valid_data(self, basic_schema):
        data = {"text": "hello", "length": 5}
        assert basic_schema.validate(data) is True

    def test_missing_required_field(self, basic_schema):
        data = {"length": 5}
        with pytest.raises(ValueError, match="Missing required field"):
            basic_schema.validate(data)

    def test_invalid_type(self, basic_schema):
        data = {"text": "hello", "length": "5"}
        with pytest.raises(TypeError):
            basic_schema.validate(data)

    def test_custom_validator(self, basic_schema):
        data = {"text": "hello", "length": -1}
        with pytest.raises(ValueError):
            basic_schema.validate(data)


class TestEnrichmentVersion:
    def test_create_version(self, basic_schema):
        config = EnrichmentVersionConfig(
            prompt="Test prompt",
            model="test-model",
            params={},
            input_schema=basic_schema,
            output_schema=basic_schema,
        )
        version = EnrichmentVersion.create(config)
        assert version.version_id is not None
        assert isinstance(version.created_at, datetime)

    def test_version_id_consistency(self, basic_schema):
        """Same inputs should produce same version ID"""
        config = EnrichmentVersionConfig(
            prompt="Test prompt",
            model="test-model",
            params={},
            input_schema=basic_schema,
            output_schema=basic_schema,
        )
        version1 = EnrichmentVersion.create(config)
        version2 = EnrichmentVersion.create(config)
        assert version1.version_id == version2.version_id


class TestEnrichmentDefinition:
    def test_add_version(self, sample_version):
        enrichment = EnrichmentDefinition(name="test", description="test enrichment")
        enrichment.add_version(sample_version)
        assert len(enrichment.versions) == 1
        assert enrichment.get_latest_version() == sample_version

    def test_get_version(self, sample_version):
        enrichment = EnrichmentDefinition(name="test", description="test enrichment")
        enrichment.add_version(sample_version)
        assert enrichment.get_version(sample_version.version_id) == sample_version
        assert enrichment.get_version("nonexistent") is None

    def test_status_transitions(self):
        enrichment = EnrichmentDefinition(name="test", description="test enrichment")
        assert enrichment.status == EnrichmentStatus.DRAFT
        enrichment.status = EnrichmentStatus.PUBLISHED
        assert enrichment.status == EnrichmentStatus.PUBLISHED
