from __future__ import annotations
from typing import Union, Dict, List, Iterable
from enum import Enum
from datetime import datetime
from dataclasses import dataclass
import re


class RCJobStatus(Enum):
    NOT_STARTED: int = -1
    IN_PROGRESS: int = 0
    FINISHED: int = 1
    FAILED: int = 2


def _fix_isotime(time: str) -> str:
    """
    The ISO time returned by rclone (at least when tested on MEGA) returns a format that datetime doesn't like
    this function fixes these issues

    :param time: a string with a ISO timestamp
    :return: A string with a re-formatted timestamp
    """

    # Defines a regular expression to find the inconsistencies
    pattern = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]*)?(\+[0-9]{2}:[0-9]{2}|[a-zA-Z])?"

    # Finds and match inconsistencies
    matches = re.search(pattern, time)

    # Fixes inconsistencies
    if matches[1] is not None:
        time = time.replace(matches[1], "")
    if (matches[2] is not None) and (not matches[2].startswith("+")):
        time = time.replace(matches[2], "")

    # Returns a new string with fixed timestamp
    return time


@dataclass
class RCloneJob:
    """
    this class contains information related to any job
    """

    id: int
    duration: float
    startTime: datetime
    endTime: Union[datetime | None]
    error: str = ""
    output: Union[str | None] = None
    finished: bool = False
    success: bool = False
    stats: Union[RCloneJobStats | None] = None

    @property
    def status(self) -> RCJobStatus:
        if (not self.finished) and (not self.success):
            return RCJobStatus.IN_PROGRESS if self.stats is not None else RCJobStatus.NOT_STARTED
        elif self.success:
            return RCJobStatus.FINISHED
        else:
            return RCJobStatus.FAILED

    @classmethod
    def _get_data_from_json(cls, json_data: Dict):
        return {
            "id": json_data['id'],
            "duration": json_data['duration'],
            "startTime": datetime.fromisoformat(_fix_isotime(json_data['startTime'])),
            "endTime": datetime.fromisoformat(_fix_isotime(json_data['endTime'])) if json_data[
                                                                                         'endTime'] != 0 else None,
            "error": json_data['error'],
            "output": json_data['output'],
            "success": json_data['success'],
            "finished": json_data['finished']
        }

    @classmethod
    def from_json(cls, json_data: Dict) -> RCloneJob:
        d = cls._get_data_from_json(json_data)
        return cls(**d)


@dataclass(frozen=True)
class RCloneJobStats:
    """
    this class collects detailed information about a job that is transferring a file
    """

    transferred_bytes: int = 0
    filename: str = ""
    size: int = 0
    speed: float = 0.
    average_speed: float = 0.

    @property
    def percentage(self) -> float:
        return self.transferred_bytes / self.size

    @classmethod
    def from_json(cls, json_data: Dict) -> RCloneJobStats:
        d = cls._get_data_from_json(json_data)
        return cls(**d)

    @classmethod
    def _get_data_from_json(cls, json_data: Dict):
        return {
            "transferred_bytes": json_data['bytes'],
            "filename": json_data['name'],
            "size": json_data['size'],
            "speed": json_data['speed'],
            "average_speed": json_data['speedAvg'],
        }


class RCloneTransferDetails:
    """
    this object collects the transfer information of many jobs, making easy to gather global information
    """

    def __init__(self, jobs: List[RCloneJobStats]):
        self._jobs = jobs

    def __len__(self) -> int:
        return len(self._jobs)

    def __getitem__(self, index: int) -> RCloneJobStats:
        """
        Return the job at the provided position
        :param index: Job index
        :return: A RCloneTransferJob
        """

        return self._jobs[index]

    def __iter__(self) -> Iterable[RCloneJobStats]:
        return iter(self._jobs)

    @property
    def percentage(self) -> float:
        transf = 0
        total = 0

        for job in self:
            transf += job.transferred_bytes
            total += job.size

        return transf / total

    @property
    def total_transfer_speed(self) -> float:
        return sum([job.speed for job in self])

    @property
    def total_average_transfer_speed(self) -> float:
        return sum([job.average_speed for job in self])
