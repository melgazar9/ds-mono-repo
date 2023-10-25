# tap-yfinance

`tap-yfinance` is a Singer tap for YFinance.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

Install from GitHub (not on PyPi yet):

```bash
pipx install git+https://github.com/melgazar9/tap-yfinance.git
```

## Configuration

### Accepted Config Options


* catalog
* state
* discover
* about
* stream-maps
* schema-flattening
* batch

A full list of supported settings and capabilities for this
tap is available by running:
```
tap-yfinance --about --format=markdown
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can run `tap-yfinance` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-yfinance --version
tap-yfinance --help
tap-yfinance --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-yfinance` CLI interface directly using `poetry run`:

```bash
poetry run tap-yfinance --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._


Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-yfinance
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-yfinance --version
# OR run a test extract and load `el` pipeline:
meltano el tap-yfinance target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
