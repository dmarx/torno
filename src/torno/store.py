# src/torno/store.py
"""torno/store.py - Main feature store implementation"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from .core import (
    EnrichmentDefinition, EnrichmentVersion, EnrichmentJob,
    FeatureSet, Schema, JobStatus
)

class FeatureStore:
    """Main interface for the Torno feature store"""
    
    def __init__(self):
        self.enrichments: Dict[str, EnrichmentDefinition] = {}
        self.jobs: List[EnrichmentJob] = []
        self.features: Dict[str, FeatureSet] = {}

    def register_enrichment(
        self, name: str, description: str, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> EnrichmentDefinition:
        """Register a new enrichment definition"""
        if name in self.enrichments:
            raise ValueError(f"Enrichment {name} already exists")
            
        enrichment = EnrichmentDefinition(
            name=name,
            description=description,
            metadata=metadata or {}
        )
        self.enrichments[name] = enrichment
        return enrichment

    def create_version(
        self, enrichment_name: str, prompt: str, model: str,
        params: Dict[str, Any], input_schema: Schema,
        output_schema: Schema, metadata: Optional[Dict[str, Any]] = None
    ) -> EnrichmentVersion:
        """Create a new version of an enrichment"""
        if enrichment_name not in self.enrichments:
            raise ValueError(f"Enrichment {enrichment_name} not found")
            
        version = EnrichmentVersion.create(
            prompt=prompt,
            model=model,
            params=params,
            input_schema=input_schema,
            output_schema=output_schema,
            metadata=metadata
        )
        
        self.enrichments[enrichment_name].add_version(version)
        return version

    def queue_job(
        self, dataset_id: str, enrichment_name: str,
        input_data: Dict[str, Any], version_id: Optional[str] = None
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
            input_data=input_data
        )
        
        self.jobs.append(job)
        return job

    def get_job(self, job_id: str) -> Optional[EnrichmentJob]:
        """Get a job by ID"""
        for job in self.jobs:
            if job.job_id == job_id:
                return job
        return None

    def update_job(
        self, job_id: str, status: Optional[JobStatus] = None,
        result: Optional[Dict[str, Any]] = None, 
        error: Optional[str] = None
    ) -> Optional[EnrichmentJob]:
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
        self, dataset_id: str, 
        enrichment_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get features for a dataset"""
        feature_set = self.features.get(dataset_id)
        if not feature_set:
            return None
            
        if enrichment_name:
            return feature_set.get_features(enrichment_name)
        return feature_set.features

    def add_features(
        self, dataset_id: str, enrichment_name: str,
        features: Dict[str, Any]
    ) -> None:
        """Add features to a dataset"""
        if dataset_id not in self.features:
            self.features[dataset_id] = FeatureSet(dataset_id=dataset_id)
        
        self.features[dataset_id].add_features(enrichment_name, features)
