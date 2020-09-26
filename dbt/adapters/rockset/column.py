from dataclasses import dataclass

from dbt.adapters.base.column import Column
from dbt.exceptions import RuntimeException

import rockset


@dataclass
class RocksetColumn(Column):
    def is_integer(self) -> bool:
        return self.dtype.lower() in ['int']

    def is_numeric(self) -> bool:
        return self.dtype.lower() in ['int', 'float']

    def is_float(self):
        return self.dtype.lower() in ['float']

    def is_string(self):
        return self.dtype.lower() in ['string']

    def string_size(self) -> int:
        if not self.is_string():
            raise RuntimeException("Called string_size() on non-string field!")

        return rockset.Client.MAX_FIELD_VALUE_BYTES
