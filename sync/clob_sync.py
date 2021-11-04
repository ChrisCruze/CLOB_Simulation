import logging
import queue
import gzip
import json

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

class OrderDictTable:
    """
        Creates a dictionary from list of orders
    """
    def __init__(self, order_items):
        self.order_dict = {} 
        self.assign_orders_dict(order_items)
        
    def assign_orders_dict(self,order_items):
        keys = [subli[2] for subli in order_items]#map(lambda price,size,order_id: order_id,order_items)
        self.order_dict = dict([(order_id,order_item) for (order_id,order_item) in zip(keys,order_items) ])

    def get_obj(self):
        return self.order_dict


class CLOB:
    """
        Applies messages to CLOB
    """
    def __init__(self,clob):
        self.bids_obj = OrderDictTable(clob['bids']).get_obj()
        self.asks_obj = OrderDictTable(clob['asks']).get_obj()
        logger.info('Loaded the initial CLOB snapshot into data structure')
        self.sequence = clob['sequence']
        
    def order_remove(self,order_obj,order_id):
        order_obj.pop(order_id,None)
        
    def order_add(self,order_obj,price,size,order_id):
        order_obj[order_id] = [price,size,order_id]
    
    def order_size_decrease(self,order_obj,order_id,size):
        previous_size = order_obj[order_id][1]
        new_size = str(float(previous_size) - float(size))
        price = order_obj[order_id][0]
        order_obj[order_id] = [price,new_size,order_id]
        
    def order_update(self,order_obj,message_obj):
        message_type = message_obj['type']
        if message_type == 'done':
            self.order_remove(order_obj,message_obj['order_id'])
        elif message_type == 'open':
            self.order_add(order_obj,message_obj['price'],message_obj['remaining_size'],message_obj['order_id'])
        elif message_type == 'match':
            self.order_size_decrease(order_obj,message_obj['maker_order_id'],message_obj['size'])

    def sequence_number_update(self,message_obj):
        self.sequence  = message_obj['sequence']

    def order_object_determine(self,message_obj):
        message_side = message_obj['side']
        if message_side == 'sell':
            order_obj = self.asks_obj
        elif message_side == 'buy' :
            order_obj = self.bids_obj
        return order_obj

    def update_from_message(self,message_obj):
        order_obj = self.order_object_determine(message_obj)
        self.order_update(order_obj,message_obj)
        self.sequence_number_update(message_obj)

    def get_clob(self):
        return {
            'bids':self.bids_obj.values(),
            'asks':self.asks_obj.values(),
            'sequence':self.sequence
        }

class CLOBSync(object):
    """
        Applies messages to initial clob by looping through each message
    """
    def clob_sync(self,initial_clob,messages_data_filtered):
        clob = CLOB(initial_clob)
        q = queue.Queue()
        messages_queue_data = sorted(messages_data_filtered,key=lambda i: i['sequence'])
        logger.info('Load and loop through the order book messages until you reach the sequence number from the initial snapshot')

        list(map(q.put,messages_queue_data ))
        while not q.empty():
            message_obj = q.get()
            clob.update_from_message(message_obj)

        logger.info('Apply the changes from messages')
        final_clob_processed = clob.get_clob()
        logger.info('Done applying messages to CLOB')
        return final_clob_processed


if __name__ == '__main__':
    initial_clob,final_clob,messages_data_filtered = DataLoad().data_load()
    updated_clob = CLOBSync().clob_sync(initial_clob,messages_data_filtered)
