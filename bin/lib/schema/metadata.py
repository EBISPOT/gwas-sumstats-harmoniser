from pydantic import (BaseModel,
                      constr)
from datetime import date
from typing import List, Optional


"""
Models
"""


class SampleMetadata(BaseModel):
    ancestryMethod: Optional[List[str]] = None
    caseControlStudy: Optional[bool] = None
    caseCount: Optional[int] = None
    controlCount: Optional[int] = None
    sampleSize: int = None
    sampleAncestry: List[str] = None


class SumStatsMetadata(BaseModel):
    genotypingTechnology: List[str] = []
    GWASID: Optional[constr(regex=r'^GCST\d+$')] = None
    traitDescription: List[str] = None
    effectAlleleFreqLowerLimit: Optional[float] = None
    dataFileName: str = None
    fileType: str = None
    dataFileMd5sum: str = None
    isHarmonised: Optional[bool] = False
    isSorted: Optional[bool] = False
    dateLastModified: date = None
    genomeAssembly: str = None
    effectStatistic: Optional[str] = None
    pvalueIsNegLog10: Optional[bool] = False
    analysisSoftware: Optional[str] = None
    imputationPanel: Optional[str] = None
    imputationSoftware: Optional[str] = None
    harmonisationReference: Optional[str] = None
    adjustedCovariates: Optional[str] = None
    ontologyMapping: Optional[str] = None
    authorNotes: Optional[str] = None
    GWASCatalogAPI: Optional[str] = None
    sex: Optional[str] = None
    coordinateSystem: Optional[str] = None
    samples: List[SampleMetadata] = []
    
    class Config:
        title = 'GWAS Summary Statistics metadata schema'