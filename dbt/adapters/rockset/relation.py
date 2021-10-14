from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy
import traceback


@dataclass
class RocksetQuotePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass
class RocksetIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class RocksetRelation(BaseRelation):
    quote_policy: RocksetQuotePolicy = RocksetQuotePolicy()
    include_policy: RocksetIncludePolicy = RocksetIncludePolicy()
