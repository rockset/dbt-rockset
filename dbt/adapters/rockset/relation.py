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
