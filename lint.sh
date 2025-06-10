#!/usr/bin/env bash
set -e

echo "Running black..."
black .
echo "Running isort..."
isort . --profile black
echo "Running flake8..."
flake8

#mypy .         # if using type hints

