# BI utils

Perfectly suits your project!

### Installation

```bash
pip install git+https://github.com/gismart/bi-utils
```

Add `--upgrade` option to update existing package to a new version

### Requirements

Specify package link in your `requirements.txt`:

```txt
git+https://github.com/gismart/bi-utils@0.6.0#egg=bi-utils-gismart
```

### Usage

If you have your credentials on AWS add the following environment variables to use DB/S3 functionality:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`

### Running tests

```bash
pytest
```
