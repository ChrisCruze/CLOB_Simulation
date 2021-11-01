import logging
import queue
# from .data_load import DataLoad
logging.basicConfig(level=20)
logger = logging.getLogger()

class OrderDictTable:
    """
        Create dictionary from list of bids or asks
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
    def __init__(self,clob):
        self.bids_obj = OrderDictTable(clob['bids']).get_obj()
        self.asks_obj = OrderDictTable(clob['asks']).get_obj()
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
        
    def update_from_message(self,message_obj):
        message_type = message_obj['type']
        message_side = message_obj['side']
        if message_side == 'sell':
            order_obj = self.asks_obj
        elif message_side == 'buy' :
            order_obj = self.bids_obj
        
        if message_type == 'done':
            self.order_remove(order_obj,message_obj['order_id'])
        elif message_type == 'open':
            self.order_add(order_obj,message_obj['price'],message_obj['remaining_size'],message_obj['order_id'])
        elif message_type == 'match' and message_side == 'buy':
            self.order_size_decrease(order_obj,message_obj['maker_order_id'],message_obj['size'])
          
        elif message_type == 'match' and message_side == 'sell':
            self.order_size_decrease(order_obj,message_obj['maker_order_id'],message_obj['size'])
            
        self.sequence  = message_obj['sequence']
            
    def get_clob(self):
        return {
            'bids':self.bids_obj.values(),
            'asks':self.asks_obj.values(),
            'sequence':self.sequence
        }


class CLOBSync(object):
    def clob_sync(self,initial_clob,messages_data_filtered):
        clob = CLOB(initial_clob)
        q = queue.Queue()
        messages_queue_data = sorted(messages_data_filtered,key=lambda i: i['sequence'])

        list(map(q.put,messages_queue_data ))
        while not q.empty():
            message_obj = q.get()
            clob.update_from_message(message_obj)
        final_clob_processed = clob.get_clob()
        logger.info('Done')
        return final_clob_processed


if __name__ == '__main__':
    pass 