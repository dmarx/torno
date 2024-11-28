# tests/test_store.py
"""Tests for Torno feature store"""

import pytest

from torno.core import JobStatus


class TestFeatureStore:
    def test_register_enrichment(self, feature_store):
        enrichment = feature_store.register_enrichment(
            name="test", description="test enrichment"
        )
        assert enrichment.name == "test"
        assert "test" in feature_store.enrichments

    def test_register_duplicate_enrichment(self, feature_store):
        feature_store.register_enrichment(name="test", description="test enrichment")
        with pytest.raises(ValueError, match="already exists"):
            feature_store.register_enrichment(
                name="test", description="duplicate enrichment"
            )

    def test_create_version(self, feature_store, basic_schema):
        feature_store.register_enrichment(name="test", description="test enrichment")
        version = feature_store.create_version(
            enrichment_name="test",
            prompt="Test prompt",
            model="test-model",
            params={},
            input_schema=basic_schema,
            output_schema=basic_schema,
        )
        assert version.version_id is not None
        assert feature_store.enrichments["test"].get_latest_version() == version

    def test_queue_job(self, feature_store, sample_enrichment):
        feature_store.enrichments["test"] = sample_enrichment
        job = feature_store.queue_job(
            dataset_id="test_dataset",
            enrichment_name="test",
            input_data={"text": "test text", "length": 9},
        )
        assert job.job_id is not None
        assert job.status == JobStatus.PENDING
        assert job in feature_store.jobs

    def test_queue_job_invalid_input(self, feature_store, sample_enrichment):
        feature_store.enrichments["test"] = sample_enrichment
        with pytest.raises(ValueError):
            feature_store.queue_job(
                dataset_id="test_dataset",
                enrichment_name="test",
                input_data={"length": 9},  # Missing required 'text' field
            )

    def test_update_job(self, feature_store, sample_enrichment):
        feature_store.enrichments["test"] = sample_enrichment
        job = feature_store.queue_job(
            dataset_id="test_dataset",
            enrichment_name="test",
            input_data={"text": "test text", "length": 9},
        )

        # Update job status
        updated_job = feature_store.update_job(
            job_id=job.job_id, status=JobStatus.RUNNING
        )
        assert updated_job.status == JobStatus.RUNNING
        assert updated_job.started_at is not None

        # Update with results
        result = {"key_points": ["point1"], "summary": "test"}
        updated_job = feature_store.update_job(
            job_id=job.job_id, status=JobStatus.COMPLETED, result=result
        )
        assert updated_job.status == JobStatus.COMPLETED
        assert updated_job.result == result
        assert updated_job.completed_at is not None

    def test_get_features(self, feature_store):
        feature_store.add_features(
            dataset_id="test",
            enrichment_name="test_enrichment",
            features={"key_points": ["point1", "point2"]},
        )

        # Get specific enrichment features
        features = feature_store.get_features(
            dataset_id="test", enrichment_name="test_enrichment"
        )
        assert features == {"key_points": ["point1", "point2"]}

        # Get all features for dataset
        all_features = feature_store.get_features(dataset_id="test")
        assert "test_enrichment" in all_features
