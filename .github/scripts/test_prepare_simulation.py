import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("prepare_simulation", Path(__file__).with_name("prepare-simulation.py"))
staging = importlib.util.module_from_spec(spec)
spec.loader.exec_module(staging)


class PrepareSimulationTest(unittest.TestCase):
    def test_independent_client_uses_common_harness_without_modifying_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, client, destination = (base / name for name in ("pr", "main", "stage"))
            simulation = root / "pkg/internal/simulation"
            (simulation / "mimir").mkdir(parents=True)
            client.mkdir()
            module = "module github.com/grafana/warpstream-go\n\ngo 1.26.0\n"
            (root / "go.mod").write_text(module)
            (root / "go.sum").write_text("")
            (client / "go.mod").write_text(module)
            (simulation / "mimir/go.mod").write_text("module example.com/mimir\n\ngo 1.26.0\n")
            source = 'package main\nimport "github.com/grafana/warpstream-go/pkg/internal/simulation/runner"\n'
            (simulation / "main.go").write_text(source)
            for ignored in (".build", ".reports"):
                (simulation / ignored).mkdir()
                (simulation / ignored / "large-file").write_text("ignored")
            staging.prepare(root, client, destination)
            self.assertEqual((simulation / "main.go").read_text(), source)
            self.assertEqual((client / "go.mod").read_text(), module)
            self.assertIn(staging.HARNESS_MODULE + "/runner", (destination / "main.go").read_text())
            self.assertIn("module " + staging.HARNESS_MODULE, (destination / "go.mod").read_text())
            self.assertIn(str(client), (destination / "go.mod").read_text())
            self.assertIn(staging.HARNESS_MODULE, (destination / "mimir/go.mod").read_text())
            self.assertIn(str(destination), (destination / "mimir/go.mod").read_text())
            self.assertFalse((destination / ".build").exists())
            self.assertFalse((destination / ".reports").exists())


if __name__ == "__main__":
    unittest.main()
