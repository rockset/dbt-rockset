from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy

import traceback
from typing import List, Optional


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

    # We override this function with a very simple implementation. Database is not a concept
    # in Rockset, so we do not make such a comparison
    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ) -> bool:
        return self.path.schema == schema and self.path.identifier == identifier
