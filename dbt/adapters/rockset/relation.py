from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy
import traceback


@dataclass
class RocksetQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class RocksetRelation(BaseRelation):
    quote_policy: RocksetQuotePolicy = RocksetQuotePolicy()

    # TODO(sam): Something is initting a rockset relation, and that may be where we're losing
    # the target schema, bc its empty by the time we get to create_schema
    def __init__(self, *args, **kwargs):
        super(RocksetRelation, self).__init__(*args, **kwargs)
