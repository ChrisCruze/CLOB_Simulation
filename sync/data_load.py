import gzip
import json
import logging 

logging.basicConfig(level=20)
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
        messages_data = self.messages_parse(data)
        logger.info('loaded %s messages', str(len(messages_data)))
        return messages_data 

    def snapshot_read(self,file="../data/coinbase_BTC-USD_20_10_06_00_00.json"):
        snaphsot_data = json.loads(open(file,'r').read())
        logger.info('loaded snapshot: %s', str(file))
        return snaphsot_data

    def messages_filter(self,messages_data,initial_clob,final_clob):
        messages_data_filtered = [message_dict for message_dict in messages_data if message_dict['sequence'] >= initial_clob['sequence'] and message_dict['sequence'] <= final_clob['sequence']]
        return messages_data_filtered

    def data_load(self):    
        messages_data = self.messages_read()
        initial_clob = self.snapshot_read(file="../data/coinbase_BTC-USD_20_10_06_00_00.json")
        final_clob = self.snapshot_read(file="../data/coinbase_BTC-USD_20_10_06_00_15.json")
        messages_data_filtered = self.messages_filter(messages_data,initial_clob,final_clob)
        return initial_clob,final_clob,messages_data_filtered

if __name__ == '__main__':
    initial_clob,final_clob,messages_data_filtered = data_load()
