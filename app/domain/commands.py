from dataclasses import dataclass


@dataclass
class SynthetizeSystem:
    variables: list[str]


@dataclass
class SynthetizeExecution:
    variables: list[str]


@dataclass
class SynthetizeOperation:
    variables: list[str]
