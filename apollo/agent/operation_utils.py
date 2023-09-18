import logging
from datetime import datetime
from typing import Optional, List, Any, Dict, Iterable, Callable

logger = logging.getLogger(__name__)


class OperationUtils:
    @classmethod
    def to_json(
        cls,
        o: Any,
        properties: Optional[List[str]] = None,
        include: bool = True,
    ) -> Any:
        if cls._is_json_serializable(o):
            return o
        elif isinstance(o, Iterable):
            try:
                return [
                    cls.to_json(i, properties=properties, include=include) for i in o
                ]
            except Exception:
                logger.debug(f"Failed to iterate property of type {type(o)}")
                return None
        else:
            return cls._serialize_object(o, properties=properties, include=include)

    @classmethod
    def _serialize_object(
        cls,
        o: Any,
        properties: Optional[List[str]],
        include: bool,
    ) -> Dict:
        result = {}
        for attr_name in filter(lambda n: not n.startswith("_"), dir(o)):
            if not cls._is_included_property(properties, include, attr_name):
                continue
            try:
                attr_value = getattr(o, attr_name)
            except Exception:
                logger.debug(f"Failed to get attribute {attr_name} from {type(o)}")
                continue
            if attr_value is None or isinstance(attr_value, Callable):
                continue
            if cls._is_json_serializable(attr_value):
                result[attr_name] = attr_value
            else:
                serialized_value = cls.serialize(
                    attr_value,
                    properties=cls._filter_properties_by_prefix(properties, attr_name),
                    include=include,
                )
                if serialized_value:
                    result[attr_name] = serialized_value
        return result

    @classmethod
    def _filter_properties_by_prefix(
        cls, properties: Optional[List[str]], attr_name: str
    ) -> Optional[List[str]]:
        if properties is None:
            return None
        prefix = f"{attr_name}."
        prefix_len = len(prefix)
        result = [prop[prefix_len:] for prop in properties if prop.startswith(prefix)]
        return result

    @staticmethod
    def _is_included_property(
        properties: Optional[List[str]],
        include: bool,
        prop_name: str,
    ):
        if not properties:
            return True
        prefix = f"{prop_name}."
        if include:
            return any(
                prop == prop_name or prop.startswith(prefix) for prop in properties
            )
        else:
            return prop_name not in properties

    @staticmethod
    def _is_json_serializable(o: Any) -> bool:
        return (
            isinstance(o, str)
            or isinstance(o, float)
            or isinstance(o, int)
            or isinstance(o, Dict)
            or isinstance(o, datetime)
        )
