#!/usr/bin/env python3
"""Stage this checkout's harness with an independently selected client module."""

import argparse
import shutil
import subprocess
from pathlib import Path

HARNESS_MODULE = "github.com/grafana/warpstream-go/pkg/simulationci"
CLIENT_MODULE = "github.com/grafana/warpstream-go"
SOURCE_MODULE = CLIENT_MODULE + "/pkg/internal/simulation"


def prepare(root, client, destination):
    root, client, destination = map(lambda p: Path(p).resolve(), (root, client, destination))
    if not (client / "go.mod").is_file():
        raise ValueError("client directory must contain go.mod")
    # A separate module lets the same harness import either client's own package.
    shutil.copytree(root / "pkg/internal/simulation", destination,
                    ignore=shutil.ignore_patterns(".build", ".reports"))
    for name in ("go.mod", "go.sum"):
        shutil.copyfile(root / name, destination / name)
    for path in destination.rglob("*.go"):
        path.write_text(path.read_text().replace(SOURCE_MODULE + "/runner", HARNESS_MODULE + "/runner"))
    subprocess.run(["go", "mod", "edit", "-module=" + HARNESS_MODULE,
                    "-require=" + CLIENT_MODULE + "@v0.0.0",
                    "-replace=" + CLIENT_MODULE + "=" + str(client)], cwd=destination, check=True)
    for module in destination.glob("*/go.mod"):
        subprocess.run(["go", "mod", "edit", "-require=" + HARNESS_MODULE + "@v0.0.0",
                        "-replace=" + HARNESS_MODULE + "=" + str(destination),
                        "-replace=" + CLIENT_MODULE + "=" + str(client)], cwd=module.parent, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("client")
    parser.add_argument("destination")
    args = parser.parse_args()
    prepare(Path(__file__).resolve().parents[2], args.client, args.destination)
