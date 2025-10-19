import csv
import datetime
import os
from decimal import Decimal
from io import StringIO
from typing import List, Tuple, Optional

import requests
from bs4 import BeautifulSoup

from beanprice import source


class ArivaError(ValueError):
    "An error from the ariva.de API."


def _make_session() -> requests.Session:
    session = requests.Session()

    r1 = session.get('https://www.ariva.de/user/login/')
    soup = BeautifulSoup(r1.text, 'html.parser')

    form = soup.find('form')
    action_url = form.attrs['action']
    username = os.getenv('ARIVA_USERNAME')
    password = os.getenv('ARIVA_PASSWORD')

    if not username or not password:
        raise ArivaError('environment variables ARIVA_USERNAME and ARIVA_PASSWORD must be set')

    post_data = {
        "username": username,
        "password": password,
    }
    response = session.post(action_url, data=post_data)

    refresh_token = session.cookies.get('kc_refresh_token')
    access_token = session.cookies.get('kc_access_token')

    if not response.ok or not refresh_token or not access_token:
        raise ArivaError('failed to authenticate with Ariva')

    return session

def _get_price_series(
    session: requests.Session,
    symbol_id: str,
    market_id: str,
    time_begin: datetime.datetime = datetime.datetime.min,
    time_end: datetime.datetime = datetime.datetime.max,
) -> List[source.SourcePrice]:


    base_url = \
        f"https://www.ariva.de/quote/historic/historic.csv"

    query = {
        'secu': symbol_id,
        'boerse_id': market_id,
        'clean_split': '1',
        'clean_payout': '1',
        'clean_bezug': '1',
        'trenner': ';',
        'go': 'Download',
    }
    if time_begin != datetime.datetime.min:
        query['min_time'] = time_begin.date().strftime("%d.%m.%Y")
    if time_end != datetime.datetime.max:
        query['max_time'] = time_end.date().strftime("%d.%m.%Y")

    response = session.get(
        base_url,
        params=query,
    )

    if response.status_code != requests.codes.ok:
        raise ArivaError(
            f"Invalid response ({response.status_code}): {response.text}"
        )

    if response.headers.get('content-type') != 'text/csv':
        raise ArivaError(
            'unexpected response content type'
        )

    f = StringIO(response.text)
    reader = csv.DictReader(f, delimiter=';')

    def to_decimal(val: str) -> Decimal:
        us_digit = val.replace(',', '.')
        d = Decimal(us_digit)
        return d.quantize(Decimal('0.0000000'))

    ret = []
    for row in reader:
        date = datetime.datetime.fromisoformat(row['Datum']).date()
        if date < time_begin.date() or date > time_end.date():
            continue
        ret.append(source.SourcePrice(
            price=to_decimal(row['Schlusskurs']),
            time=datetime.datetime(
                date.year,
                date.month,
                date.day,
                tzinfo=datetime.timezone.utc,
            ),
            quote_currency='EUR',
        ))
    return ret


def _parse_ticker(ticker: str) -> Tuple[str, str]:
    if ticker.count(':') != 1:
        raise ArivaError('ticker must be in the format "SYMBOL_ID:MARKET_ID"')
    symbol_id, market_id = ticker.split(':', maxsplit=1)
    return symbol_id, market_id


class Source(source.Source):
    def __init__(self):
        self._session: Optional[requests.Session] = None

    def get_session(self) -> requests.Session:
        if self._session:
            return self._session

        self._session = _make_session()
        return self._session

    def get_latest_price(self, ticker):
        symbol_id, market_id = _parse_ticker(ticker)
        prices = _get_price_series(self.get_session(), symbol_id, market_id)
        if len(prices) < 1:
            return None
        return prices[-1]

    def get_historical_price(self, ticker, time):
        symbol_id, market_id = _parse_ticker(ticker)
        prices = _get_price_series(self.get_session(), symbol_id, market_id, time, time)
        if len(prices) < 1:
            return None
        return prices[0]

    def get_prices_series(self, ticker, time_begin, time_end):
        symbol_id, market_id = _parse_ticker(ticker)
        prices = _get_price_series(self.get_session(), symbol_id, market_id, time_begin, time_end)
        if len(prices) < 1:
            return None
        return prices
