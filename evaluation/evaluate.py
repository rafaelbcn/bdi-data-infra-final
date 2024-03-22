import json
import os
import subprocess

import pytest
from mypy import api
from pydantic import BaseModel, Field

pytest_report = ".pytest.json"
cov_report = ".coverage.json"
mypy_report = ".mypy"


class ExerciseEvaluation(BaseModel):
    STUDENT_TEST_POINTS: int = 2
    TEST_POINTS: int = 6
    CLEAN_POINTS: int = 1
    PERFORMANCE_POINTS: int = 1

    student: str = Field(description="Student name")
    repository: str
    commit: str = Field(description="Commit hash used to evaluate")
    exercise: str = Field(description="exercise number")
    test: float = 0
    coverage: float = 0
    cleanliness: float = 0
    performance: float = 0

    def evaluate(self):
        self.run_student_test()
        self.run_ruff()
        self.run_mypy()

    @property
    def total_grade(self):
        return self.test + self.coverage + self.cleanliness + self.performance

    def run_student_test(self) -> None:
        pytest_return = pytest.main(
            [
                "-q",
                f"tests/{self.exercise}",
                "--json-report",
                "--json-report-summary",
                f"--json-report-file={pytest_report}",
                f"--cov=bdi_api/{self.exercise}",
                "--cov-report",
                f"json:{cov_report}",
            ],
        )
        if pytest_return != 0:
            self.coverage = 0.0

        with open(pytest_report) as f:
            data = json.load(f)
            summary = data["summary"]

        failed_ratio = 1 - summary["passed"] / summary["collected"]

        with open(cov_report) as f:
            data = json.load(f)
            summary = data["totals"]
        cov_ratio = summary["percent_covered"] / 100.0
        if summary["excluded_lines"] > 9:
            cov_ratio = 0.0

        self.coverage = round(max(0.0, self.STUDENT_TEST_POINTS * (cov_ratio - failed_ratio)), 1)

    def run_mypy(self):
        # https://mypy.readthedocs.io/en/stable/extending_mypy.html
        api.run(["bdi_api/s1", "--txt-report", mypy_report, "--linecount-report", "."])

    def run_ruff(self, linter_points: int = 2, max_allowed: int = 10):
        from ruff.__main__ import find_ruff_bin

        ruff = find_ruff_bin()
        completed_process = subprocess.run([os.fsdecode(ruff), "check", "."], capture_output=True)

        num_errors = 0
        out_lines = completed_process.stdout.decode().split("\n")
        for line in out_lines:
            if line.startswith("Found"):
                num_errors = int(line.split(" ")[1])

        if num_errors >= max_allowed:
            self.cleanliness = 0.0
        self.cleanliness = round(max(0.0, linter_points * (1 - num_errors / max_allowed * 1.0)), 1)

    def __str__(self):
        return f"""Evaluation result for {self.student}
\tTest:\t{self.test}/6
\tCove:\t{self.coverage}/2
\tLint:\t{self.cleanliness}/1
\tPerf:\t{self.performance}/1
=========
TOTAL:\t{self.total_grade}
"""


if __name__ == "__main__":
    exercise = ExerciseEvaluation(
        student="Marti Segarra",
        repository="",
        commit="",
        exercise="examples",
    )
    exercise.evaluate()

    print(exercise)
