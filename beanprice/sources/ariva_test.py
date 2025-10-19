import datetime
import decimal
import unittest

from beanprice.sources import ariva

SYMBOL_ID_A1JX52 = '108506260'
MARKET_ID_TRADEGATE = '131'



class ArivaPriceFetcher(unittest.TestCase):
    def setUp(self):
        # reset the Decimal context since other tests override this
        decimal.getcontext().prec = 12
        decimal.getcontext().rounding = decimal.ROUND_HALF_UP
        self.sut = ariva.Source()

    def test_valid_response(self):
        price_1 = self.sut.get_prices_series(
            f'{SYMBOL_ID_A1JX52}:{MARKET_ID_TRADEGATE}',
            datetime.datetime(2024, 10, 19),
            datetime.datetime(2025, 10, 19),
        )


if __name__ == "__main__":
    unittest.main()
