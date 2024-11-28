# torno
Feature store for LLM data enrichment

# Anticipated Project Structure

```
torno/
├── .git/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── publish.yml
├── src/
│   └── torno/
│       ├── __init__.py
│       ├── core.py            # Core data models
│       ├── store.py           # Feature store implementation
│       ├── api/
│       │   ├── __init__.py
│       │   ├── routes.py      # FastAPI routes
│       │   └── models.py      # API models/schemas
│       ├── cli/
│       │   ├── __init__.py
│       │   └── main.py        # CLI implementation
│       ├── workers/
│       │   ├── __init__.py
│       │   ├── base.py        # Worker base class
│       │   └── llm.py         # LLM-specific worker
│       ├── storage/
│       │   ├── __init__.py
│       │   ├── base.py        # Storage interface
│       │   ├── memory.py      # In-memory implementation
│       │   ├── sql.py         # SQL implementation
│       │   └── redis.py       # Redis implementation
│       └── utils/
│           ├── __init__.py
│           ├── logging.py     # Logging utilities
│           └── validation.py  # Validation utilities
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Pytest fixtures
│   ├── test_core.py
│   ├── test_store.py
│   └── test_workers.py
├── docs/
│   ├── index.md
│   ├── installation.md
│   ├── usage.md
│   └── api-reference.md
├── examples/
│   ├── basic_usage.py
│   └── custom_enrichment.py
├── .gitignore
├── .pre-commit-config.yaml
├── LICENSE
├── MANIFEST.in
├── README.md
├── pyproject.toml
└── mkdocs.yml
```
