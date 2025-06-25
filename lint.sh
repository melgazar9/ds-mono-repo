#!/usr/bin/env bash
set -e

echo "Running black..."
black .

echo "Running flake8..."
flake8

echo "Running pre-commit hooks..."
pre-commit run --all-files --show-diff-on-failure
