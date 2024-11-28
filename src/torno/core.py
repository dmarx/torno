# core.py
"""torno/core.py - Core data models and interfaces for the Torno feature store"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
import hashlib
import json
from uuid import uuid4


class EnrichmentStatus(Enum):
    """Status of an enrichment definition"""

    DRAFT = "draft"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"


class JobStatus(Enum):
    """Status of an enrichment job"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Schema:
    """Schema definition for enrichment inputs/outputs"""
    fields: Dict[str, str]
    validators: Dict[str, Any] = field(default_factory=dict)
    required: List[str] = field(default_factory=list)

    def validate(self, data: Dict[str, Any]) -> bool:
        """Validate data against schema"""
        # Check required fields
        for required_field in self.required:
            if required_field not in data:
                raise ValueError(f"Missing required field: {required_field}")
        
        # Validate types and custom validators
        for field_name, value in data.items():
            if field_name not in self.fields:
                raise ValueError(f"Unknown field: {field_name}")
            
            expected_type = self.fields[field_name]
            if not isinstance(value, eval(expected_type)):
                raise TypeError(f"Field {field_name} expected {expected_type}, got {type(value)}")
            
            if field_name in self.validators:
                validator_result = self.validators[field_name](value)
                if validator_result is False:
                    raise ValueError(f"Validation failed for field: {field_name}")
        
        return True

@dataclass
class EnrichmentVersionConfig:
    """Configuration for an enrichment version"""
    prompt: str
    model: str
    params: Dict[str, Any]
    input_schema: Schema
    output_schema: Schema
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class EnrichmentVersion:
    """Version of an enrichment definition"""
    version_id: str
    created_at: datetime
    prompt_template: str
    model_id: str
    parameters: Dict[str, Any]
    input_schema: Schema
    output_schema: Schema
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(cls, config: EnrichmentVersionConfig) -> 'EnrichmentVersion':
        """Create a new enrichment version with a unique ID"""
        # Remove creation time from version hash to ensure consistency
        version_data = {
            "prompt": config.prompt,
            "model": config.model,
            "params": config.params,
            "input_schema": config.input_schema.fields,
            "output_schema": config.output_schema.fields,
            "metadata": config.metadata or {},
        }
        
        version_id = hashlib.sha256(
            json.dumps(version_data, sort_keys=True).encode()
        ).hexdigest()[:12]
        
        return cls(
            version_id=version_id,
            created_at=datetime.utcnow(),
            prompt_template=config.prompt,
            model_id=config.model,
            parameters=config.params,
            input_schema=config.input_schema,
            output_schema=config.output_schema,
            metadata=config.metadata or {}
        )


@dataclass
class EnrichmentDefinition:
    """Definition of an enrichment pipeline"""

    name: str
    description: str
    versions: list[EnrichmentVersion] = field(default_factory=list)
    status: EnrichmentStatus = EnrichmentStatus.DRAFT
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def add_version(self, version: EnrichmentVersion) -> None:
        """Add a new version to this enrichment"""
        self.versions.append(version)
        self.updated_at = datetime.utcnow()

    def get_version(self, version_id: str) -> EnrichmentVersion | None:
        """Get a specific version by ID"""
        for version in self.versions:
            if version.version_id == version_id:
                return version
        return None

    def get_latest_version(self) -> EnrichmentVersion | None:
        """Get the most recent version"""
        return self.versions[-1] if self.versions else None


@dataclass
class EnrichmentJob:
    """Job for applying an enrichment to input data"""

    job_id: str
    dataset_id: str
    enrichment_name: str
    enrichment_version: str
    status: JobStatus
    created_at: datetime
    input_data: dict[str, Any]
    result: dict[str, Any] | None = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        dataset_id: str,
        enrichment_name: str,
        enrichment_version: str,
        input_data: dict[str, Any],
    ) -> "EnrichmentJob":
        """Create a new enrichment job"""
        return cls(
            job_id=str(uuid4()),
            dataset_id=dataset_id,
            enrichment_name=enrichment_name,
            enrichment_version=enrichment_version,
            status=JobStatus.PENDING,
            created_at=datetime.utcnow(),
            input_data=input_data,
        )


@dataclass
class FeatureSet:
    """Collection of enriched features for a dataset"""

    dataset_id: str
    features: dict[str, dict[str, Any]]  # enrichment_name -> feature_data
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def add_features(self, enrichment_name: str, features: dict[str, Any]) -> None:
        """Add features from an enrichment"""
        self.features[enrichment_name] = features
        self.updated_at = datetime.utcnow()

    def get_features(self, enrichment_name: str) -> dict[str, Any] | None:
        """Get features for a specific enrichment"""
        return self.features.get(enrichment_name)
