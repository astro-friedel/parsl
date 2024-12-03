"""This module implements DataFutures.
"""
import logging
import os.path
from concurrent.futures import Future
from datetime import datetime, timezone
from hashlib import md5
from os import stat
from typing import TYPE_CHECKING, Optional

import typeguard

from parsl.data_provider.files import File

if TYPE_CHECKING:
    from parsl.dataflow.dflow import DataFlowKernel

logger = logging.getLogger(__name__)


class DataFuture(Future):
    """A datafuture points at an AppFuture.

    We are simply wrapping a AppFuture, and adding the specific case where, if
    the future is resolved i.e. file exists, then the DataFuture is assumed to be
    resolved.
    """

    def parent_callback(self, parent_fu):
        """Callback from executor future to update the parent.

        Updates the future with the result (the File object) or the parent future's
        exception.

        Args:
            - parent_fu (Future): Future returned by the executor along with callback

        Returns:
            - None
        """
        e = parent_fu._exception
        if e:
            self.set_exception(e)
        else:
            self.set_result(self.file_obj)
            # only update the file object if it is a file
            if self.data_flow_kernel.file_provenance and self.file_obj.scheme == 'file' and os.path.isfile(self.file_obj.filepath):
                if not self.file_obj.timestamp:
                    self.file_obj.timestamp = datetime.fromtimestamp(stat(self.file_obj.filepath).st_ctime, tz=timezone.utc)
                if not self.file_obj.size:
                    self.file_obj.size = stat(self.file_obj.filepath).st_size
                if not self.file_obj.md5sum:
                    self.file_obj.md5sum = md5(open(self.file_obj, 'rb').read()).hexdigest()
            self.data_flow_kernel.register_as_output(self.file_obj, self.app_fut.task_record)

    @typeguard.typechecked
    def __init__(self, fut: Future, file_obj: File, dfk: "DataFlowKernel", tid: Optional[int] = None, app_fut: Optional[Future] = None) -> None:
        """Construct the DataFuture object.

        If the file_obj is a string convert to a File.

        Args:
            - fut (AppFuture) : AppFuture that this DataFuture will track
            - file_obj (string/File obj) : Something representing file(s)

        Kwargs:
            - tid (task_id) : Task id that this DataFuture tracks
        """
        super().__init__()
        self._tid = tid
        if isinstance(file_obj, File):
            self.file_obj = file_obj
        else:
            raise ValueError("DataFuture must be initialized with a File, not {}".format(type(file_obj)))
        self.parent = fut
        if app_fut:
            self.app_fut = app_fut
        else:
            self.app_fut = fut
        self.data_flow_kernel = dfk
        self.parent.add_done_callback(self.parent_callback)
        # only capture this if needed
        if self.data_flow_kernel.file_provenance and self.file_obj.scheme == 'file' and os.path.exists(file_obj.path):
            file_stat = os.stat(file_obj.path)
            self.file_obj.timestamp = datetime.fromtimestamp(file_stat.st_ctime, tz=timezone.utc)
            self.file_obj.size = file_stat.st_size
            self.file_obj.md5sum = md5(open(self.file_obj, 'rb').read()).hexdigest()

        logger.debug("Creating DataFuture with parent: %s and file: %s", self.parent, repr(self.file_obj))

    @property
    def tid(self):
        """Returns the task_id of the task that will resolve this DataFuture."""
        return self._tid

    @property
    def filepath(self):
        """Filepath of the File object this datafuture represents."""
        return self.file_obj.filepath

    @property
    def filename(self):
        """Filepath of the File object this datafuture represents."""
        return self.filepath

    @property
    def uuid(self):
        """UUID of the File object this datafuture represents."""
        return self.file_obj.uuid

    @property
    def timestamp(self):
        """Timestamp when the future was marked done."""
        return self.file_obj.timestamp

    @timestamp.setter
    def timestamp(self, value: Optional[datetime]) -> None:
        self.file_obj.timestamp = value

    @property
    def size(self):
        """Size of the file."""
        return self.file_obj.size

    @property
    def md5sum(self):
        """MD5 sum of the file."""
        return self.file_obj.md5sum

    def cancel(self):
        raise NotImplementedError("Cancel not implemented")

    def cancelled(self):
        return False

    def running(self):
        if self.parent:
            return self.parent.running()
        else:
            return False

    def __repr__(self) -> str:
        type_ = type(self)
        module = type_.__module__
        qualname = type_.__qualname__
        if self.done():
            done = "done"
        else:
            done = "not done"
        return f"<{module}.{qualname} object at {hex(id(self))} representing {repr(self.file_obj)} {done}>"
