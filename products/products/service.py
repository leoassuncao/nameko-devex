import logging

from marshmallow import ValidationError
from nameko.events import event_handler
from nameko.rpc import rpc
from nameko.web.handlers import http

from products import dependencies, schemas
from products.exceptions import NotFound


logger = logging.getLogger(__name__)


class ProductsService:

    name = 'products'

    storage = dependencies.Storage()

    @rpc
    def get(self, product_id):
        try:
            product = self.storage.get(product_id)
            if product is None:
                return 404, {"error": "Product not found"}
            return schemas.Product().dump(product).data
        except NotFound as e:
            raise e
        except Exception as e:
            return e

    @rpc
    def list(self):
        try:
            products = self.storage.list()
            return schemas.Product(many=True).dump(products).data
        except Exception as e:
            logger.exception("An error occurred")
            raise e

    @rpc
    def product_id_list(self, product_ids):
        try:
            products = self.storage.filter_by_ids(product_ids)
            return schemas.Product(many=True).dump(products).data
        except Exception as e:
            logger.exception("An error occurred")
            return e

    @rpc
    def create(self, product):
        try:
            product = schemas.Product(strict=True).load(product).data
            self.storage.create(product)
        except Exception as e:
            return 500, {"error": "An error occurred"}


    @rpc
    def delete(self, product_id):
        try:
            self.storage.delete(product_id)
        except Exception as e:
            logger.exception("An error occurred")
            return e

    @event_handler('orders', 'order_created')
    def handle_order_created(self, payload):
        try:
            for product in payload['order']['order_details']:
                self.storage.decrement_stock(
                    product['product_id'], product['quantity'])
        except Exception as e:
            return 500, {"error": "An error occurred"}
