# xAI models

Download of sales information from Opensea. Normalisation of sales by trend. Model of sales.


## Setup

```
python3.9 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install wheel
pip install -e ".[dev]"
```

## API Key

The opensea API requires an API key, that can be optained from here:
https://docs.opensea.io/reference/api-keys

## Notebooks

* [Opensea](notebooks/opensea.ipynb): Download of NFT sales information from Opensea.
* [Parse](notebooks/parse.ipynb): Parsing of the raw JSON data into seperate dataframes.
* [Trend](notebooks/trend.ipynb): Computation of price tend and creation of an adjusted price.
* [Model](notebooks/model.ipynb): Simple linear model predicting the adjusted price based on NFT features.
