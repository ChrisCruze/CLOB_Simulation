import sys, os 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
from sync import CLOBSync
from sync import DataLoad
import logging 
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(level=logging.INFO,format=LOG_FORMAT,filemode="w")
logger = logging.getLogger()

class TestSync(unittest.TestCase):
	@classmethod
	def setUpClass(self):
		initial_clob,final_clob,messages_data_filtered = DataLoad().data_load()
		self.final_clob = final_clob
		self.updated_clob = CLOBSync().clob_sync(initial_clob,messages_data_filtered)

	def test_bids_length_equal(self):
		final_bids = len(self.final_clob['bids'])
		updated_bids = len(self.updated_clob['bids'])
		self.assertEqual(updated_bids,final_bids)
		logger.info('verified bids count matches: %s bids in final snapshot, %s bids in updated snapshot', str(final_bids),str(updated_bids))

	def test_asks_length_equal(self):
		final_asks = len(self.final_clob['asks'])
		updated_asks = len(self.updated_clob['asks'])
		self.assertEqual(updated_asks,final_asks)
		logger.info('verified asks count matches: %s asks in final snapshot, %s asks in updated snapshot', str(final_asks),str(updated_asks))

	def test_bids_are_identical(self):
		final_bids = self.final_clob['bids']
		updated_bids = self.updated_clob['bids']
		final_bids_not_in_updated = [order for order in final_bids if order not in updated_bids]
		updated_bids_not_in_final = [order for order in updated_bids if order not in final_bids]
		bids_difference = final_bids_not_in_updated + updated_bids_not_in_final
		self.assertEqual(len(bids_difference),0)
		logger.info('verified no differences in bids')

	def test_asks_are_identical(self):
		final_asks = self.final_clob['asks']
		updated_asks = self.updated_clob['asks']
		final_asks_not_in_updated = [order for order in final_asks if order not in updated_asks]
		updated_asks_not_in_final = [order for order in updated_asks if order not in final_asks]
		asks_difference = final_asks_not_in_updated + updated_asks_not_in_final
		self.assertEqual(len(asks_difference),0)
		logger.info('verified no differences in asks')

	def test_sequence_number_is_identical(self):
		final_sequence_number = self.final_clob['sequence']
		updated_sequence_number = self.updated_clob['sequence']
		self.assertEqual(updated_sequence_number,final_sequence_number)
		logger.info('verified sequence number is the same between final and updated')


if __name__ == '__main__':
    unittest.main()