from __future__ import annotations

from typing import Any, Callable, TypeVar

T = TypeVar("T")


class _PipelinesShim:

    @staticmethod
    def table(*args: Any, **kwargs: Any) -> Callable[[T], T]:
        def _decorator(func: T) -> T:
            return func

        return _decorator

    @staticmethod
    def materialized_view(*args: Any, **kwargs: Any) -> Callable[[T], T]:
        def _decorator(func: T) -> T:
            return func

        return _decorator

    @staticmethod
    def expect_or_fail(*args: Any, **kwargs: Any) -> Callable[[T], T]:
        def _decorator(func: T) -> T:
            return func

        return _decorator


dp = _PipelinesShim()
