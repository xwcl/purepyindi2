import typing
from .constants import ANY

class IndiServer:
    def __init__(
        self,
        bind_host: str,
        bind_port: int,
        remote_drivers: list[str],
        settable: typing.Optional[list[str]] = None,
        visible: typing.Union[list[str], ANY, None] = ANY,
    ):
        pass
