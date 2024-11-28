# src/torno/store.py
"""torno/store.py - Main feature store implementation"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .core import (
    EnrichmentDefinition,
    EnrichmentJob,
    EnrichmentVersion,
    EnrichmentVersionConfig,
    FeatureSet,
    JobStatus,
)


@dataclass
class EnrichmentRegistration:
    """Data for registering a new enrichment"""

    name: str
    description: str
    metadata: dict[str, Any] | None = None


class FeatureStore:
    """Main interface for the Torno feature store"""

    def __init__(self):
        self.enrichments: dict[str, EnrichmentDefinition] = {}
        self.jobs: list[EnrichmentJob] = []
        self.features: dict[str, FeatureSet] = {}

    def register_enrichment(
        self, registration: EnrichmentRegistration
    ) -> EnrichmentDefinition:
        """Register a new enrichment definition"""
        if registration.name in self.enrichments:
            raise ValueError(f"Enrichment {registration.name} already exists")

        enrichment = EnrichmentDefinition(
            name=registration.name,
            description=registration.description,
            metadata=registration.metadata or {},
        )
        self.enrichments[registration.name] = enrichment
        return enrichment

    def create_version(
        self, enrichment_name: str, config: EnrichmentVersionConfig
    ) -> EnrichmentVersion:
        """Create a new version of an enrichment"""
        if enrichment_name not in self.enrichments:
            raise ValueError(f"Enrichment {enrichment_name} not found")

        version = EnrichmentVersion.create(config)
        self.enrichments[enrichment_name].add_version(version)
        return version

    def queue_job(
        self,
        dataset_id: str,
        enrichment_name: str,
        input_data: dict[str, Any],
        version_id: str | None = None,
    ) -> EnrichmentJob:
        """Queue a new enrichment job"""
        enrichment = self.enrichments.get(enrichment_name)
        if not enrichment:
            raise ValueError(f"Enrichment {enrichment_name} not found")

        if version_id:
            version = enrichment.get_version(version_id)
            if not version:
                raise ValueError(f"Version {version_id} not found")
        else:
            version = enrichment.get_latest_version()
            if not version:
                raise ValueError(f"No versions found for enrichment {enrichment_name}")

        # Validate input data against schema
        version.input_schema.validate(input_data)

        job = EnrichmentJob.create(
            dataset_id=dataset_id,
            enrichment_name=enrichment_name,
            enrichment_version=version.version_id,
            input_data=input_data,
        )

        self.jobs.append(job)
        return job

    def get_job(self, job_id: str) -> EnrichmentJob | None:
        """Get a job by ID"""
        for job in self.jobs:
            if job.job_id == job_id:
                return job
        return None

    def update_job(
        self,
        job_id: str,
        status: JobStatus | None = None,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> EnrichmentJob | None:
        """Update a job's status and results"""
        job = self.get_job(job_id)
        if not job:
            return None

        if status:
            job.status = status
            if status == JobStatus.RUNNING:
                job.started_at = datetime.utcnow()
            elif status in (JobStatus.COMPLETED, JobStatus.FAILED):
                job.completed_at = datetime.utcnow()

        if result is not None:
            # Validate output against schema
            enrichment = self.enrichments[job.enrichment_name]
            version = enrichment.get_version(job.enrichment_version)
            if version:
                version.output_schema.validate(result)
            job.result = result

        if error is not None:
            job.error = error

        return job

    def get_features(
        self, dataset_id: str, enrichment_name: str | None = None
    ) -> dict[str, Any] | None:
        """Get features for a dataset"""
        feature_set = self.features.get(dataset_id)
        if not feature_set:
            return None

        if enrichment_name:
            return feature_set.get_features(enrichment_name)
        return feature_set.features

    def add_features(
        self, dataset_id: str, enrichment_name: str, features: dict[str, Any]
    ) -> None:
        """Add features to a dataset"""
        if dataset_id not in self.features:
            self.features[dataset_id] = FeatureSet(dataset_id=dataset_id)

        self.features[dataset_id].add_features(enrichment_name, features)
