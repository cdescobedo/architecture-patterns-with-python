import abc

from sqlalchemy import text
import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session: str):
        self.session = session

    def add(self, batch: model.Batch):
        batch_insert = self.session.execute(
            text(
                "INSERT INTO batches (reference, sku, _purchased_quantity, eta)"
                " VALUES (:batch_id, :batch_sku, :batch_pq, :batch_eta)"
            ),
            dict(
                batch_id=batch.reference,
                batch_sku=batch.sku,
                batch_pq=batch.available_quantity,
                batch_eta=batch.eta,
            ),
        )

        for order_line in batch._allocations:
            order_line_insert = self.session.execute(
                text(
                    "INSERT INTO order_lines (sku, qty, orderid)"
                    " VALUES (:sku, :qty, :orderid)"
                ),
                dict(
                    sku=order_line.sku,
                    qty=order_line.qty,
                    orderid=order_line.orderid,
                ),
            )

            self.session.execute(
                text(
                    "INSERT INTO allocations (orderline_id, batch_id)"
                    " VALUES (:orderline_id, :batch_id)"
                ),
                dict(
                    orderline_id=order_line_insert.lastrowid,
                    batch_id=batch_insert.lastrowid,
                ),
            )

    def get(self, reference: str) -> model.Batch:
        batch_row = self.session.execute(
            text(
                "SELECT reference as ref, sku, _purchased_quantity as qty, eta"
                " FROM batches"
                " WHERE reference=:reference"
            ),
            dict(reference=reference),
        ).one()

        allocation_rows = self.session.execute(
            text(
                "SELECT orderid, order_lines.sku, order_lines.qty"
                " FROM allocations"
                " JOIN order_lines ON allocations.orderline_id = order_lines.id"
                " JOIN batches ON allocations.batch_id = batches.id"
                " WHERE batches.reference = :batchid"
            ),
            dict(batchid=reference),
        ).all()

        batch = model.Batch(**batch_row._mapping)
        for row in allocation_rows:
            batch.allocate(model.OrderLine(**row._mapping))

        return batch
