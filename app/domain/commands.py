from dataclasses import dataclass
from typing import List


@dataclass
class SynthetizeSystem:
    variables: List[str]


@dataclass
class SynthetizeExecution:
    variables: List[str]


@dataclass
class SynthetizeScenario:
    variables: List[str]


@dataclass
class SynthetizeOperation:
    variables: List[str]
