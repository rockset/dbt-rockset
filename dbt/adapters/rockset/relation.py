from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class RocksetQuotePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class RocksetRelation(BaseRelation):
    include_policy: RocksetQuotePolicy = RocksetQuotePolicy()
    quote_policy: RocksetQuotePolicy = RocksetQuotePolicy()
