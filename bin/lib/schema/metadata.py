from pydantic import BaseModel, validator
from typing import List, Optional
from datetime import date
from enum import Enum


class EffectStatisticEnum(str, Enum):
    beta = 'beta'
    odds_ratio = 'odds_ratio'
    hazard_ratio = 'hazard_ratio'


class SampleMetadata(BaseModel):
    ancestryMethod: Optional[List[str]] = None
    caseControlStudy: Optional[bool] = None
    caseCount: Optional[int] = None
    controlCount: Optional[int] = None
    sampleSize: int = None
    sampleAncestry: List[str] = None

    @validator('ancestryMethod', 'sampleAncestry', pre=True)
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split('|')
        return v

    @validator('caseControlStudy', pre=True)
    def str_to_bool(cls, v):
        if str(v).lower() in {'no', 'false', 'n'}:
            return False
        elif str(v).lower() in {'yes', 'true', 'y'}:
            return True
        else:
            return v


class SumStatsMetadata(BaseModel):
    genotypingTechnology: List[str] = []
    GWASID: str = None
    traitDescription: List[str] = None
    effectAlleleFreqLowerLimit: Optional[float] = None
    dataFileName: str = None
    fileType: str = None
    dataFileMd5sum: str = None
    isHarmonised: Optional[bool] = False
    isSorted: Optional[bool] = False
    dateLastModified: date = None
    genomeAssembly: str = None
    effectStatistic: Optional[EffectStatisticEnum] = None
    pvalueIsNegLog10: Optional[bool] = False
    analysisSoftware: Optional[str] = None
    imputationPanel: Optional[str] = None
    imputationSoftware: Optional[str] = None
    hmCodeDefinition: Optional[dict] = None
    harmonisationReference: Optional[str] = None
    adjustedCovariates: Optional[str] = None
    ontologyMapping: Optional[str] = None
    authorNotes: Optional[str] = None
    GWASCatalogAPI: Optional[str] = None
    samples: List[SampleMetadata] = []

    @validator('genotypingTechnology', 'traitDescription', pre=True)
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split('|')
        return v

    @validator('isHarmonised', 'isSorted', 'pvalueIsNegLog10',
               pre=True)
    def str_to_bool(cls, v):
        if str(v).lower() in {'no', 'false', 'n'}:
            return False
        elif str(v).lower() in {'yes', 'true', 'y'}:
            return True
        else:
            return v

    class Config:
        use_enum_values = True  
