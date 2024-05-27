from typing import List
from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass(frozen=True)
class RCloneAuthenticator(ABC):
    username: str = ""
    passoword: str = ""

    @property
    @abstractmethod
    def cl_arguments(self) -> List[str]:
        ...


@dataclass(frozen=True)
class RCloneUserAuthenticator(RCloneAuthenticator):

    @property
    def cl_arguments(self) -> List[str]:
        return [
            "--rc-user", self.username,
            "--rc-pass", self.passoword
        ]
