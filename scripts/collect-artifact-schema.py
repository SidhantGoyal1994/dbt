#!/usr/bin/env python
from dataclasses import dataclass
from argparse import ArgumentParser
from pathlib import Path
import json
from typing import Type, List

from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.results import (
    CatalogArtifact, RunResultsArtifact, FreshnessExecutionResultArtifact
)
from dbt.contracts.util import VersionedSchema
from dbt.clients.system import write_file
from dbt.utils import JSONEncoder

# TODO(kwigley): we could look for subclasses instead
artifacts: List[Type[VersionedSchema]] = [
    WritableManifest,
    CatalogArtifact,
    RunResultsArtifact,
    FreshnessExecutionResultArtifact
]


@ dataclass
class Arguments:
    path: Path

    @ classmethod
    def parse(cls) -> 'Arguments':
        parser = ArgumentParser(
            prog="Collect and write dbt arfifact schema"
        )
        parser.add_argument(
            '--path',
            type=Path,
            help='The dir to write artifact schema',
            required=True
        )
        parsed = parser.parse_args()
        return cls(
            path=parsed.path
        )


def collect_artifact_schema(args: Arguments):
    artifacts_path = args.path.absolute()
    for artifact_cls in artifacts:
        file_path = artifacts_path / artifact_cls.dbt_schema_version.path
        write_file(str(file_path), json.dumps(
            artifact_cls.json_schema(), cls=JSONEncoder))


def main():
    parsed = Arguments.parse()
    collect_artifact_schema(parsed)


if __name__ == '__main__':
    main()
