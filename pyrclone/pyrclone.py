import asyncio

from typing_extensions import Self, AsyncIterable
from typing import Union, Tuple, Any, List, Dict
from subprocess import Popen, PIPE
from aiohttp import ClientSession, BasicAuth, ClientResponseError, ClientTimeout
from aiohttp.client_exceptions import ClientConnectorError
from .auth import RCloneAuthenticator
from .jobs import RCloneJob, RCloneJobStats, RCJobStatus
import json
import os


class Rclone:

    def __init__(self,
                 cmd: str,
                 address: str = "localhost",
                 port: int = 5572,
                 authentication: bool = False,
                 authenticator: Union[RCloneAuthenticator | None] = None
                 ):
        """
        RClone remote controller class. It either uses an already running rclone deamon, or starts its own via the
        `run` method

        Raises a ValueError when `authentication` is True and no authenticator was provided

        :param cmd: rclone command. If rclone binaries are in system Path, the string `rclone` is enough.
                    Alternatively, a full path to the binary should be provided.

        :param address: IP address of the server (either to connect or bind)
        :param port: Port where the server (will) listen
        :param authentication: TRUE if authentication is used, otherwise FALSE
        :param authenticator: An RCloneAuthenticator object
        """

        self._cmd = cmd
        self._address = address
        self._port = port
        self._auth = None

        if authentication:
            if authenticator is None:
                ValueError("You must provide an authenticator if the parameter `authentication` is TRUE.")

            self._auth = authenticator

        self._running_server: Union[Popen | None] = None
        self._session: Union[ClientSession | None] = None
        self._transferring_jobs: List[int] = []
        self._transferring_jobs_last_update: Dict[int, Union[RCloneJob | None]] = dict()

    async def __aenter__(self) -> Self:
        self.run()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.quit()
        return exc_type is None

    @property
    def _http_session(self) -> ClientSession:
        """
        Returns the current HTTP session
        :return: A ClientSession object
        """
        if self._session is None:
            args = {"base_url": f"http://{self._address}:{self._port}", "timeout": ClientTimeout(total=10)}

            if self._auth is not None:
                auth = BasicAuth(login=self._auth.username, password=self._auth.passoword)
                args['auth'] = auth

            self._session = ClientSession(**args)

        return self._session

    async def make_request(self,
                           backend: str,
                           command: str,
                           **kwargs) -> Any:
        """
        Make a request to the RClone Daemon
        :param backend: RClone backend
        :param command: Supported command within the backend
        :param kwargs: Anything supported by backend/command
        :return: A dictionary representing the json response provided by RClone
        """
        async with self._http_session.post(f"/{backend}/{command}", ssl=False, json=kwargs) as response:
            content = await response.text(encoding="utf-8")
            if response.status == 200:
                return json.loads(content)
            else:
                raise ClientResponseError(response.request_info, response.history, message=content)

    async def ls(self, root: str, path: str, recursive: bool = False) -> Any:
        """
        返回根目录中给定路径下的文件列表

        如果根/路径不存在，则引发 "FileNotFoundError
        如果出现与客户端/服务器连接相关的任何问题，则引发 ClientResponseError 错误

        :param root： RClone远程或本地路径
        :param path：从根开始的相对路径
        :param recursive：如果为 “true”，运行根/路径中所有文件的递归列表
        :return： 包含目录内容的列表
        """

        opt = {}

        if recursive:
            opt['recurse'] = True

        path = path.lstrip("./")  # rclone doesn't like paths startign with . or / (or both!)

        try:
            data = await self.make_request("operations",
                                           "list",
                                           fs=root,
                                           remote=path,
                                           opt=opt)
            return data['list']

        except ClientResponseError as err:
            if "directory not found" in err.message:
                raise FileNotFoundError(f"{os.path.join(root, path)} was not found")
            else:
                raise err

    async def about(self, root: str):
        """
        返回根目录中给定路径下的网盘储存情况

        如果根不存在，则引发FileNotFoundError错误
        如果出现与客户端/服务器连接相关的问题，则引发 ClientResponseError 错误

        :param root： RClone远程或本地路径
        :return： 网盘储存情况
        """

        opt = {}

        try:
            data = await self.make_request("operations",
                                           "about",
                                           fs=root,
                                           opt=opt)
            return data

        except ClientResponseError as err:
            if "directory not found" in err.message:
                raise FileNotFoundError(f"{os.path.join(root)} was not found")
            else:
                raise err

    async def exists(self, root: str, path: str) -> bool:
        """
        Check if the provided file/directory exists

        :param root: An RClone remote or a local path
        :param path: a path relative from root
        :return: TRUE if the file exists, FALSE otherwise
        """

        return (await self.stat(root, path)) is not None

    async def stat(self, root: str, path: str) -> Any:
        """
        Give information about the supplied file or directory

        :param root: An RClone remote or a local path
        :param path: a path relative from root
        :return: A json containing information about the file/directory, None otherwise
        """

        data = await self.make_request("operations",
                                       "stat",
                                       fs=root,
                                       remote=path)
        return data['item']

    async def list_remotes(self):
        """
        Return the list of remotes

        Raises ClientResponseError for any issues related to client/server connection

        :return:
        """

        d = await self.make_request('config', 'dump', long=True)

        remotes = []

        for k in d:
            remotes.append((d[k]['type'], f"{k}:"))

        return remotes

    async def checksum(self, path: str, hash: str = "md5", remote: bool = False) -> Union[str | None]:
        """
        Calculate the checksum of a file. this command doesn't use remote control as this command is only available
        from the classic comamnd line. Why? Dunno!

        :param path: Path to the file to get its checksum
        :param hash: The list of supported hashes is here: https://rclone.org/commands/rclone_hashsum/
        :param remote: Remotes might not support the calculation of hashes. Hence, files need to be dowloaded.
                       Set thizs parameter TRUE wisely as some cloud storage services can limit download bandwidth
        :return:a string representing the hash of the file
        """

        args = ["hashsum", hash, path]

        if remote:
            args.append("--download")

        proc = await asyncio.create_subprocess_exec(self._cmd, *args,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await proc.communicate()

        if proc.returncode == 0:
            return stdout.decode().split(" ")[0]

    async def copy_file(self, src_root, src_path, dst_root, dst_path) -> int:
        """
        Copy a file

        :param src_root: Source root path
        :param src_path: Source path to filename
        :param dst_root: Destination root path
        :param dst_path: Destination path to filename
        :return:
        """
        request_data = {
            "srcFs": src_root,
            "srcRemote": src_path,
            "dstFs": dst_root,
            "dstRemote": dst_path,
            "_async": "true"
        }

        response = await self.make_request("operations", "copyfile", **request_data)

        id = response['jobid']
        self._transferring_jobs.append(id)

        return id

    async def copy(self, srcFs: str, dstFs: str, createEmptySrcDirs=True):
        """
        srcFs - 远程名称字符串，例如源的“drive：src”
        dstFs - 远程名称字符串，例如目标的“drive：dst”
        createEmptySrcDirs - 如果设置了，则在目标上创建空的 src 目录
        _async - 异步操作允许
        """
        request_data = {
            "srcFs": srcFs,
            "dstFs": dstFs,
            "createEmptySrcDirs": createEmptySrcDirs,
            "_async": "true"
        }
        response = await self.make_request("sync", "copy", **request_data)

        id = response['jobid']
        self._transferring_jobs.append(id)

        return id

    async def rmdir(self, root: str, path: str, *, asynch=False) -> Self:
        """
        Delete the provided directory (it must be empty)

        :param root: An RClone remote or a local path
        :param path: a path relative from root
        :param asynch: launch thizs task asynchronously (rclone perspective)
        :return: thizs object
        """

        await self.make_request("operations",
                                "rmdir",
                                fs=root,
                                remote=path,
                                _async=asynch)
        return self

    async def delete_file(self, root: str, path: str, *, asynch=False) -> Self:
        """
        Delete a specific file

        :param root: An RClone remote or a local path
        :param path: a path relative from root
        :param asynch: launch thizs task asynchronously (rclone perspective)
        :return: thizs object
        """

        await self.make_request("operations",
                                "deletefile",
                                fs=root,
                                remote=path,
                                _async=asynch)
        return self

    @property
    async def jobs(self) -> AsyncIterable[Tuple[int, RCJobStatus]]:

        rclone_current_jobs = await self.get_rclone_job_ids()

        for jobid in self._transferring_jobs:

            if jobid in rclone_current_jobs:
                self._transferring_jobs_last_update.setdefault(jobid, None)

                try:
                    job_status = await self.get_job_status(jobid)
                    self._transferring_jobs_last_update[jobid] = job_status
                except (ClientResponseError, asyncio.TimeoutError):
                    ...
                    # job_status = self._transferring_jobs_last_update[jobid]

            job_status = self._transferring_jobs_last_update[jobid]

            if job_status is not None:
                status = job_status.status
            else:
                status = RCJobStatus.NOT_STARTED

            yield jobid, status

    async def get_rclone_job_ids(self) -> List[int]:
        request = await self.make_request("job", "list")

        return [int(x) for x in request['jobids']]

    async def has_finished(self) -> bool:
        async for id, status in self.jobs:
            if status not in [RCJobStatus.FINISHED, RCJobStatus.FAILED]:
                return False

        return True

    async def get_job_status(self, id: int) -> RCloneJob:
        """
        Get the status of a job that is transferring a file
        Differently than the `get_job_status`, thizs method retrieves more detailed information about the file
        transferring, such as bytes transferred, transfer speed, etc.

        :param id: Job id to get the information from
        :return: An RCloneTransferJob object
        """
        response_status = await self.make_request("job", "status", jobid=id)
        job = RCloneJob.from_json(response_status)

        response_stats = await self.make_request("core", "stats", group=f"job/{id}")

        if 'transferring' in response_stats.keys():
            stats = RCloneJobStats.from_json(response_stats['transferring'][0])
            job.stats = stats

        return job

    async def get_group_list(self) -> AsyncIterable[str]:
        response = await self.make_request("core", "group-list")

        if ("groups" in response) and (response["groups"] is not None):
            for x in response["groups"]:
                yield x

    async def delete_group_stats(self, group_id: str) -> bool:
        response = await self.make_request("core", "stats-delete", group=group_id)

        return len(response) == 0

    def get_last_status_update(self, jobid) -> Union[RCloneJobStats | None]:
        return self._transferring_jobs_last_update[
            jobid] if jobid in self._transferring_jobs_last_update.keys() else None

    #
    def clean_terminated_jobs(self) -> Self:
        """
        Clean the terminated jobs from the cache

        :return: thizs object
        """

        id_to_remove = []

        for jobid, last_updates in self._transferring_jobs_last_update.items():
            if last_updates.status in [RCJobStatus.FINISHED, RCJobStatus.FAILED]:
                id_to_remove.append(jobid)

        for jobid in id_to_remove:
            del self._transferring_jobs_last_update[jobid]
            self._transferring_jobs.remove(jobid)

        return self

    async def stop_job(self, jobid: int) -> bool:
        """
        Allows to stop a specific job
        :param jobid: The job id to stop

        :return: TRUE if successful, FALSE otherwise
        """
        response = await self.make_request("job", "stop", jobid=jobid)

        return len(response) == 0  # in case of success, rclone returns an empty json (weird, but it's what it is)

    async def stop_pending_jobs(self) -> Self:
        """
        Stop all pending jobs

        :return: thizs object
        """
        async for id, status in self.jobs:
            if status in [RCJobStatus.NOT_STARTED, RCJobStatus.IN_PROGRESS]:
                await self.stop_job(id)

                # sendign the command to stop a job doesn't mean it gets done immediately
                # to avoid race conditions, better double check if it gets stopped for sure
                finished = False
                while not finished:
                    stats = await self.get_job_status(id)
                    finished = stats.finished

        return self

    def run(self) -> Self:
        """
        Run the rclone remote control daemon
        thizs method is intentionally blocking

        :return: The object itself
        """
        cmd = [
            self._cmd,
            "rcd",
            "--rc-addr", f"{self._address}:{self._port}"
        ]

        if self._auth is None:
            cmd.append("--rc-no-auth")
        else:
            cmd += self._auth.cl_arguments

        self._running_server = self._running_server = Popen(
            cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE
        )

        return self

    async def is_ready(self) -> bool:
        try:
            await self.make_request("rc", "noop")
            return True
        except ClientConnectorError:
            return False

    def kill(self) -> Self:
        """
        Kill the server. thizs method should be used only when the current object launched the daemon AND it is
        unresponsive

        Raises a ChildProcessError if the daemon was not launched by the object itself

        :return: The object itself
        """

        if self._running_server is None:
            raise ChildProcessError("Unable to kill a process that was not run before.")

        self._running_server.kill()

        # The stdout of the process must be read in full after killing it, otherwise the process will turn in a
        # zombie process (at least, in a POSIX environment).
        self._running_server.communicate()

        self._running_server = Nones

        return self

    async def quit(self) -> Self:
        """
        Quit nicely the server
        :return: The object itself
        """

        await self.make_request("core", "quit")
        await self._http_session.close()

        try:
            self.kill()
            return self
        except ChildProcessError:
            ...  # If the daemon was run externally, ie not from self object, it will raise an exception. Nothing to worry about
