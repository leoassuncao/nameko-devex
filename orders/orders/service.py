import logging

from typing import KeysView
from nameko.events import EventDispatcher
from nameko.rpc import rpc
from nameko_sqlalchemy import DatabaseSession

from orders.exceptions import NotFound
from orders.models import DeclarativeBase, Order, OrderDetail
from orders.schemas import OrderSchema


class OrdersService:
    name = 'orders'

    db = DatabaseSession(DeclarativeBase)
    event_dispatcher = EventDispatcher()

    @rpc
    def get_order(self, order_id):
        try:
            order = self.db.query(Order).get(order_id)

            if not order:
                raise NotFound('Order with id {} not found'.format(order_id))

            return OrderSchema().dump(order).data
        except Exception as e:
            logger.exception("Error occurred while getting order {order_id}")   
            raise e

    @rpc
    def create_order(self, order_details):
        try:
            order = Order(
                order_details=[
                    OrderDetail(
                        product_id=order_detail['product_id'],
                        price=order_detail['price'],
                        quantity=order_detail['quantity']
                    )
                    for order_detail in order_details
                ]
            )
            self.db.add(order)
            self.db.commit()

            order = OrderSchema().dump(order).data

            self.event_dispatcher('order_created', {
                'order': order,
            })

            return order
        except Exception as e:
            logger.exception("Error occurred while creating order")
            raise e

    @rpc
    def update_order(self, order):
        try: 
            order_details = {
                order_details['id']: order_details
                for order_details in order['order_details']
            }

            order = self.db.query(Order).get(order['id'])

            for order_detail in order.order_details:
                order_detail.price = order_details[order_detail.id]['price']
                order_detail.quantity = order_details[order_detail.id]['quantity']

            self.db.commit()
            return OrderSchema().dump(order).data
        except Exception as e:
            logger.exception("Error occurred while updating order")
            raise e

    @rpc
    def delete_order(self, order_id):
        try:
            order = self.db.query(Order).get(order_id)
            self.db.delete(order)
            self.db.commit()
        except Exception as e:
            raise e    

    @rpc
    def list_orders(self, page=1, page_size=10):
        try:
            offset = (page - 1) * page_size

            orders = self.db.query(Order).offset(offset).limit(page_size).all() 

            total = self.db.query(Order).count()

            result = {
                'total': total,
                'page': page,
                'page_size': page_size,
                'orders': OrderSchema(many=True).dump(orders).data
            }

            return result
        except Exception as e:
            logger.exception("Error occurred while listing orders")
            raise e