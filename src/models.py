# src/models.py
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class SlowTask(BaseModel):
    node: Optional[str]
    time_min: float
    description: str

class SlowTasksResponse(BaseModel):
    threshold_minutes: int
    tasks: List[SlowTask]

class TemplateDiagnostic(BaseModel):
    name: str
    index_count: int
    total_docs: int
    total_size_str: str
    diagnostics_str: str

class TemplateAnalysisResponse(BaseModel):
    templates: List[TemplateDiagnostic]

class DustyShardInfo(BaseModel):
    index: str
    shard: str
    node: Optional[str]

class DustyAnalysisResponse(BaseModel):
    threshold_mb: int
    empty_shards: List[DustyShardInfo]
    dusty_shards: List[Dict[str, Any]] 