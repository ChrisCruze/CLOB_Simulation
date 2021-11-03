import gzip
import json
import logging 

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(level=logging.INFO,format=LOG_FORMAT,filemode="w")
logger = logging.getLogger()

class DataLoad(object):
    def messages_parse(self,data):
        data_with_no_binary = str(data).split("'")[1:-1][0]
        data_list =[i for i in data_with_no_binary.split('\\n') if i != '']
        messages_data = [json.loads(D) for D in data_list]
        return messages_data 

    def messages_read(self,file="../data/coinbase_BTC-USD_20_10_06_000000-010000.json.gz"):
        file_object = gzip.open(file, "r")
        data = file_object.read()
        logger.info('loaded messages data: %s', str(file))
        messages_data = self.messages_parse(data)
        logger.info('loaded %s messages', str(len(messages_data)))
        return messages_data 

    def snapshot_read(self,file="../data/coinbase_BTC-USD_20_10_06_00_00.json"):
        snaphsot_data = json.loads(open(file,'r').read())
        logger.info('loaded snapshot data: %s', str(file))
        return snaphsot_data

    def messages_filter(self,messages_data,initial_clob,final_clob):
        messages_data_filtered = [message_dict for message_dict in messages_data if message_dict['sequence'] >= initial_clob['sequence'] and message_dict['sequence'] <= final_clob['sequence']]
        logger.info('filter messages for sequence number after initial sequence number and before sequence number of final clob')
        return messages_data_filtered

    def data_load(self):
        """Returns the initial and final CLOB snapshot datasets, as well as messages filtered for sequence number"""
        messages_data = self.messages_read(file="../data/coinbase_BTC-USD_20_10_06_000000-010000.json.gz")
        initial_clob = self.snapshot_read(file="../data/coinbase_BTC-USD_20_10_06_00_00.json")
        final_clob = self.snapshot_read(file="../data/coinbase_BTC-USD_20_10_06_00_15.json")
        messages_data_filtered = self.messages_filter(messages_data,initial_clob,final_clob)
        return initial_clob,final_clob,messages_data_filtered

if __name__ == '__main__':
    initial_clob,final_clob,messages_data_filtered = data_load()
