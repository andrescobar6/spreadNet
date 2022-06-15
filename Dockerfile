FROM python:3.7

ENV PYTHONUNBUFFERED 1

# Prepare environment
RUN mkdir -p /srv/www/app
RUN mkdir -p /var/run/secrets

RUN apt-get update; apt-get install -y --no-install-recommends \
    binutils

RUN pip3 install --upgrade pip

RUN pip3 install ccxt pandas currencylayer ipython google-cloud-storage python-dotenv trading-api-wrappers surbtc pandas_gbq email-to tqdm

# Copy files
ADD currencyLayer.py /srv/www/app/currencyLayer.py
ADD spreadNet_ARS_USDC.py /srv/www/app/spreadNet_ARS_USDC.py
ADD spreadNet_CLP_BCH.py /srv/www/app/spreadNet_CLP_BCH.py
ADD spreadNet_COP_BTC.py /srv/www/app/spreadNet_COP_BTC.py
ADD spreadNet_PEN_ETH.py /srv/www/app/spreadNet_PEN_ETH.py

RUN chmod +x /srv/www/app/currencyLayer.py
RUN chmod +x /srv/www/app/*.py

WORKDIR /srv/www/app
