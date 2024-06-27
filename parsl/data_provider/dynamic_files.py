"""This module implements the DynamicFileList class and DynamicFile subclass.

The DynaicFile class is a drop in replacement/wrapper for the File and DataFuture classes. See
the XXXXXX documentation for specifics.

The DynamicFileList class is intended to replace the list of Files for app `outputs`. It acts like a
traditional Python `list`, but is also a Future. This allows for Files to be appended to the output list
and have these Files properly treated by Parsl.
"""
from __future__ import annotations
from concurrent.futures import Future
from datetime import datetime, timezone
from typing import List, Optional, Union, Callable, Dict

import typeguard
import logging

from parsl.data_provider.files import File
from parsl.app.futures import DataFuture
from parsl.dataflow.futures import AppFuture

logger = logging.getLogger(__name__)


class DynamicFileList(Future, list):
    """A list of files that is also a Future.

    This is used to represent the list of files that an app will produce.
    """

    class DynamicFile(Future):
        """A wrapper for a File or DataFuture


        """
        def parent_callback(self, parent_fu: Future):
            """Callback from executor future to update the parent.

            Updates the future with the result (the File object) or the parent future's
            exception.

            Args:
                - parent_fu (Future): Future returned by the executor along with callback

            Returns:
                - None
            """
            e = parent_fu.exception()
            if e:
                self.set_exception(e)
            else:
                self.file_obj.timestamp = datetime.now(timezone.utc)
                self.parent.dataflow.register_as_output(self.file_obj, self.parent.task_record)
                self.set_result(self.file_obj)

        def __init__(self, fut: DynamicFileList, file_obj: Optional[Union[File, DataFuture]] = None):
            """Construct a DynamicFile instance

            If the file_obj is None, create an emptry instance, otherwise wrap file_obj.

            Args:
                - fut (AppFuture) : AppFuture that this DynamicFile will track
                - file_obj (File/DataFuture obj) : Something representing file(s)
            """  # TODO need to be able to link output and input dynamic file objects and update when the output changes
            super().__init__()
            self._is_df = isinstance(file_obj, DataFuture)
            self.parent = fut
            self.file_obj = file_obj
            self.parent.add_done_callback(self.parent_callback)
            self._empty = file_obj is None       #: Tracks whether this wrapper is empty
            self._staged_out = False

        @property
        def staged(self):
            """Return whether this file has been staged out."""
            return self._staged_out

        @property
        def empty(self):
            """Return whether this is an empty wrapper."""
            return self._empty

        @property
        def uuid(self):
            """Return the uuid of the file object this datafuture represents."""
            if self._empty:
                return None
            return self.file_obj.uuid

        @property
        def timestamp(self):
            """Return the timestamp of the file object this datafuture represents."""
            if self._empty:
                return None
            return self.file_obj.timestamp

        @typeguard.typechecked
        def set(self, file_obj: Union[File, DataFuture, 'DynamicFileList.DynamicFile']):
            """Set the file_obj for this instance.

            Args:
                - file_obj (File/DataFuture) : File or DataFuture to set
            """
            if isinstance(file_obj, type(self)):
                self.file_obj = file_obj.file_obj
            self.file_obj = file_obj
            self._empty = False
            self._is_df = isinstance(self.file_obj, DataFuture)
            self.parent.add_done_func(self.file_obj.filename, self.done)

        def convert_to_df(self):
            """Convert the file_obj to a DataFuture."""
            if not self._is_df:
                self.file_obj = DataFuture(self.parent, self.file_obj, tid=self.parent._output_task_id)
                self._is_df = True

        def done(self) -> bool:
            """Return whether the file_obj state is `done`.

            Returns:
                - bool : True if the file_obj is `done`, False otherwise
            """
            if self._is_df:
                return self.file_obj.done()
            return True  # Files are always done

        @property
        def is_df(self) -> bool:
            """Return whether this instance wraps a DataFuture."""
            return self._is_df

        @property
        def tid(self):
            """Returns the task_id of the task that will resolve this DataFuture."""
            if self._is_df:
                return self.file_obj.tid

        @property
        def filepath(self):
            """Filepath of the File object this datafuture represents."""
            return self.file_obj.filepath

        @property
        def filename(self):
            """Filename of the File object this datafuture represents."""
            if self.file_obj is None:
                return None
            return self.file_obj.filepath

        def cancel(self):
            """Not implemented"""
            raise NotImplementedError("Cancel not implemented")

        def cancelled(self) -> bool:
            """Return False"""
            return False

        def running(self) -> bool:
            """Return whether the parent future is running"""
            if self.parent is not None:
                return self.parent.running()
            else:
                return False

        def exception(self, timeout=None):
            """Return None"""
            return None

        def __repr__(self) -> str:
            return self.file_obj.__repr__()

    def parent_callback(self, parent_fu):
        """Callback from executor future to update the parent.

        Updates the future with the result (the File object) or the parent future's
        exception.

        Args:
            - parent_fu (Future): Future returned by the executor along with callback

        Returns:
            - None
        """
        e = parent_fu.exception()
        if e:
            self.set_exception(e)
        else:
            self.parent._outputs = self
            self.set_result(self)

    '''''
    def file_callback(self, file_fu: Future):
        """Callback from executor future to update the file.

        Updates the future with the result (the File object) or the parent future's
        exception.

        Args:
            - file_fu (Future): Future returned by the executor along with callback

        Returns:
            - None
        """

        e = file_fu.exception()
        if e:
            self.files_done[file_fu.filename] = False
        else:
            self.files_done[file_fu.filename] = file_fu.done()
    '''
    @typeguard.typechecked
    def __init__(self, files: Optional[List[Union[File, DataFuture, DynamicFile]]] = None):
        """Construct a DynamicFileList instance

        Args:
            - files (List[File/DataFuture]) : List of files to initialize the DynamicFileList with
            - fut (Future) : Future to set as the parent
        """
        super().__init__()
        self.files_done: Dict[str, Callable] = {}      #: dict mapping file names to their "done" status True/False
        self._last_idx = -1
        self.executor: str = ''
        self.parent: Union[AppFuture, None] = None
        self.dataflow = None
        self._sub_callbacks: List[Callable] = []
        self._in_callback = False
        self._staging_inhibited = False
        self._output_task_id = None
        self.task_record = None
        if files is not None:
            self.extend(files)

    def add_done_func(self, name: str, func: Callable):
        """ Add a function to the files_done dict, specifically for when an empty DynamicFile
        is updated to contain a real File.

        Args:
            - name (str) : Name of the file to add the function for
            - func (Callable) : Function to add
        """
        self.files_done[name] = func

    def stage_file(self, idx: int):
        """ Stage a file at the given index, we do this now becuase so that the app and dataflow
        can act accordingly when the app finishes.

        Args:
            - idx (int) : Index of the file to stage
        """
        if self.dataflow is None:
            return
        out_file = self[idx]
        if out_file.empty or out_file.staged:
            return
        if self.parent is None or not out_file.is_df:
            return
        if self._staging_inhibited:
            logger.debug("Not performing output staging for: {}".format(repr(out_file.file_obj)))
        else:
            f_copy = out_file.file_obj.file_obj.cleancopy()
            self[idx].file_obj.file_obj = f_copy
            logger.debug("Submitting stage out for output file {}".format(repr(out_file.file_obj)))
            stageout_fut = self.dataflow.data_manager.stage_out(f_copy, self.executor, self.parent)
            if stageout_fut:
                logger.debug("Adding a dependency on stageout future for {}".format(repr(out_file)))
                self[idx].file_obj.parent = stageout_fut
                self[idx].file_obj._tid = self.parent.tid
            else:
                logger.debug("No stageout dependency for {}".format(repr(f_copy)))
                # self.parent._outputs.append(DataFuture(self.parent, out_file.file_obj.file_obj, tid=self.parent.tid))
            func = self.dataflow.tasks[self._output_task_id]['func']
            # this is a hook for post-task stage-out
            # note that nothing depends on the output - which is maybe a bug
            # in the not-very-tested stage-out system?
            func = self.dataflow.data_manager.replace_task_stage_out(f_copy, func, self.executor)
            self.dataflow.tasks[self._output_task_id]['func'] = func
        self.parent._outputs = self
        self._call_callbacks()
        # TODO dfk._gather_all_deps

    def wrap(self, file_obj: Union[File, DataFuture, None]):
        """ Wrap a file object in a DynamicFile

        Args:
            - file_obj (File/DataFuture) : File or DataFuture to wrap
        """
        return self.DynamicFile(self, file_obj)

    def set_dataflow(self, dataflow, executor: str, st_inhibited: bool, task_id: int, task_record: dict):
        """ Set the dataflow and executor for this instance

        Args:
            - dataflow (DataFlowKernel) : Dataflow kernel that this instance is associated with
            - executor (str) : Executor that this instance is associated with
            - st_inhibited (bool) : Whether staging is inhibited
        """
        self.executor = executor
        self.dataflow = dataflow
        self._staging_inhibited = st_inhibited
        self._output_task_id = task_id
        self.task_record = task_record
        for idx in range(self._last_idx + 1):
            self.stage_file(idx)

    def set_parent(self, fut: AppFuture):
        """ Set the parent future for this instance

        Args:
            - fut (Future) : Future to set as the parent
        """
        if self.parent is not None:
            raise ValueError("Parent future already set")
        self.parent = fut
        self.parent.add_done_callback(self.parent_callback)
        for idx in range(self._last_idx + 1):
            self[idx].convert_to_df()
            self.stage_file(idx)
        self._call_callbacks()

    def cancel(self):
        """ Not implemented """
        raise NotImplementedError("Cancel not implemented")

    def cancelled(self):
        """ Not implemented """
        return False

    def running(self):
        """ Returns True if the parent future is running """
        if self.parent is not None:
            return self.parent.running()
        else:
            return False

    def result(self, timeout=None):
        """ Return self, which is the results of the file list """
        return self

    def exception(self, timeout=None):
        """ No-op"""
        return None

    def done(self):
        """ Return True if all files are done """
        for element in self.files_done.values():
            if not element():
                return False
        return True

    @typeguard.typechecked
    def append(self, __object: Union[File, DataFuture, 'DynamicFileList.DynamicFile']):
        """ Append a file to the list and update the files_done dict

        Args:
            - __object (File/DataFuture) : File or DataFuture to append
        """
        if not isinstance(__object, DynamicFileList.DynamicFile):
            if self.parent is not None and isinstance(__object, File):
                __object = DataFuture(self.parent, __object, tid=self._output_task_id)
            __object = self.wrap(__object)
        if self._last_idx == len(self) - 1:
            super().append(__object)
        else:
            # must assume the object is empty, but exists
            super().__getitem__(self._last_idx + 1).set(__object)
        self.files_done[__object.filename] = super().__getitem__(self._last_idx + 1).done
        self._last_idx += 1
        self.stage_file(self._last_idx)
        self._call_callbacks()

    def extend(self, __iterable):
        """ Extend the list with the contents of the iterable and update the files_done dict

        Args:
            - __iterable (Iterable) : Iterable to extend the list with
        """
        items = []
        for f in __iterable:
            if not isinstance(f, (DynamicFileList.DynamicFile, File, DataFuture)):
                raise ValueError("DynamicFileList can only contain Files or DataFutures")
            if not isinstance(f, DynamicFileList.DynamicFile):
                if self.parent is not None and isinstance(f, File):
                    f = DataFuture(self.parent, f, tid=self._output_task_id)
                f = self.wrap(f)
            self.files_done[f.filename] = f.done
            items.append(f)
        if self._last_idx == len(self) - 1:
            super().extend(items)
            for i in range(len(items)):
                self._last_idx += 1
                self.stage_file(self._last_idx)
            self._call_callbacks()
            return
        diff = len(self) - 1 - self._last_idx - len(items)
        if diff < 0:
            super().extend([self.wrap(None)] * abs(diff))
        for item in items:
            self._last_idx += 1
            self[self._last_idx].set(item)
            self.files_done[item.filename] = super().__getitem__(self._last_idx).done
            self.stage_file(self._last_idx)
        self._call_callbacks()

    def insert(self, __index: int, __object: Union[File, DataFuture, DynamicFile]):
        """ Insert a file into the list at the given index and update the files_done dict

        Args:
            - __index (int) : Index to insert the file at
            - __object (File/DataFuture) : File or DataFuture to insert
        """
        if __index > self._last_idx:
            raise ValueError("Cannot insert at index greater than the last index")
        if not isinstance(__object, self.DynamicFile):
            if self.parent is not None and isinstance(__object, File):
                __object = DataFuture(self.parent, __object, tid=self._output_task_id)
            __object = self.wrap(__object)
        self.files_done[__object.filename] = __object.done
        super().insert(__index, __object)
        self.stage_file(__index)
        self._last_idx += 1
        self._call_callbacks()

    def remove(self, __value):
        """ Remove a file from the list and update the files_done dict

        Args:
            - __value (File/DataFuture) : File or DataFuture to remove
        """
        del self.files_done[__value.filename]
        super().remove(__value)
        self._last_idx -= 1
        self._call_callbacks()

    def pop(self, __index: int = -1) -> DataFuture:
        """ Pop a file from the list and update the files_done dict

        Args:
            - __index (int) : Index to pop the file at

        Returns:
            - File/DataFuture : File or DataFuture that was popped
        """
        if __index == -1:
            value = super().pop(self._last_idx)
        elif __index <= self._last_idx:
            value = super().pop(__index)
        else:
            raise IndexError("Index out of range")
        del self.files_done[value.filename]
        self._last_idx -= 1
        self._call_callbacks()
        return value.file_obj

    def clear(self):
        """ Clear the list and the files_done dict """
        self.files_done.clear()
        self._last_idx = -1
        super().clear()
        # detach all the callbacks so that sub-lists can still be used
        self._sub_callbacks.clear()

    def _call_callbacks(self):
        """ Call the callbacks for the sublists """
        if self._in_callback:
            return
        self._in_callback = True
        for cb in self._sub_callbacks:
            cb()
        self._in_callback = False

    def _expand(self, idx):
        for _ in range(idx - len(self) + 1):
            super().append(self.wrap(None))

    @typeguard.typechecked
    def __setitem__(self, key: int, value: Union[File, DataFuture, 'DynamicFileList.DynamicFile']):
        if self[key].filename in self.files_done:
            del self.files_done[self[key].filename]
        if super().__getitem__(key).empty:
            if self.parent is not None and isinstance(value, File):
                value = DataFuture(self.parent, value, tid=self._output_task_id)
            super().__getitem__(key).set(value)
            self.files_done[super().__getitem__(key).filename] = super().__getitem__(key).done
            self._last_idx = max(self._last_idx, key)
            self._call_callbacks()
            self.stage_file(key)
        else:
            raise ValueError("Cannot set a value that is not empty")
            # if not isinstance(value, self.DynamicFile):
            #    if isinstance(value, File):
            #        value = DataFuture(self.parent, value, tid=self._output_task_id)
            #    value = self.wrap(value)
            # super().__setitem__(key, value)
            # self.files_done[value.filename] = value.done

    def __getitem__(self, key):
        # make sure the list will be long enough when it is filled, so we can return a future
        if isinstance(key, slice):
            if key.start is None:
                pass
            elif key.start >= len(self):
                for i in range(len(self), key.start + 1):
                    self.append(self.wrap(None))
            if key.stop is not None and key.stop > len(self):
                for i in range(len(self), key.stop):
                    self.append(self.wrap(None))
            ret = DynamicFileSubList(key, super().__getitem__(key), self)
            self._sub_callbacks.append(ret.callback)
            return ret
        else:
            if key >= len(self):
                self._expand(key)
            return super().__getitem__(key)

    def get_update(self, key: slice):
        """Get an updated slice for the sublist.

        Args:
            - key (slice) : Slice to update

        Returns:
            - List[DynamicFile] : Updated slice
        """
        return super().__getitem__(key)

    def __delitem__(self, key):
        raise Exception("Cannot delete from a DynamicFileList")
        # del self.files_done[self[key].filename]
        # super().__delitem__(key)
        # self._call_callbacks()

    def __repr__(self):
        type_ = type(self)
        module = type_.__module__
        qualname = type_.__qualname__
        if self.done():
            done = "done"
        else:
            done = "not done"
        return f"<{module}.{qualname} object at {hex(id(self))} containing {len(self)} objects {done}>"


class DynamicFileSubList(DynamicFileList):
    @typeguard.typechecked
    def __init__(self, key: slice, files: Optional[List[DynamicFileList.DynamicFile]], parent: DynamicFileList):
        super().__init__(files=files)
        self.parent = parent
        self.slice = key
        self.fixed_size = key.stop is not None and key.start is not None

    def callback(self):
        """Callback for updating the sublist when the parent list is updated."""
        self.clear()
        self.extend(self.parent.get_update(self.slice))
