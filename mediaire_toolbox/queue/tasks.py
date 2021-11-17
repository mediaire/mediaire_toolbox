from typing import Optional
import time
import json

from copy import deepcopy


class Task(object):
    """Defines task objects that can be handled by the task manager."""

    def __init__(self,
                 t_id: Optional[int] = None,
                 user_id: Optional[int] = None,
                 product_id: Optional[int] = None,
                 tag: Optional[str] = None,
                 data: Optional[dict] = None,
                 timestamp: Optional[float] = None,
                 update_timestamp: Optional[float] = None,
                 error: Optional[str] = None,
                 site_id: int = 0):
        """Initializes the Task object.

        Parameters
        ----------
        t_id
            transaction id this task belongs to
        user_id
            user_id who submitted this task, if applicable.
        product_id
            product_id of the product
        tag
            String specifying the task. Unique for each task.
        data
            Data for specific products
        timestamp
            Timestamp of task creation from`time.time()`
        update_timestamp
            Timestamp of task update (via `create_child()`) from `time.time()`
        error
            a serialized error string in case the task failed while executing
        site_id
            ID of the site (PACS) that the task is associated for use with
            multisite setup (PO-322)
        """
        self.t_id = t_id
        self.site_id = site_id
        self.user_id = user_id
        self.product_id = product_id
        self.tag = tag
        self.timestamp = timestamp or int(time.time())
        self.update_timestamp = update_timestamp
        self.data = data
        self.error = error
        # self.update = None

    def to_dict(self):
        return {'tag': self.tag,
                'timestamp': self.timestamp,
                'update_timestamp': self.update_timestamp,
                'data': self.data,
                't_id': self.t_id,
                'site_id': self.site_id,
                'user_id': self.user_id,
                'product_id': self.product_id,
                'error': self.error}

    def to_json(self):
        return json.dumps(self.to_dict())

    def to_bytes(self):
        return self.to_json().encode('utf-8')

    def read_dict(self, d):
        tag = d['tag']
        timestamp = d['timestamp']
        t_id = d.get('t_id', None)
        site_id = d.get('site_id')  # None is implicit, default set in __init__
        user_id = d.get('user_id', None)
        product_id = d.get('product_id', None)
        update_timestamp = d.get('update_timestamp', None)
        data = d.get('data', None)
        error = d.get('error', None)
        Task.__init__(self,
                      t_id=t_id,
                      site_id=site_id,
                      user_id=user_id,
                      product_id=product_id,
                      tag=tag,
                      data=data,
                      timestamp=timestamp,
                      update_timestamp=update_timestamp,
                      error=error)
        return self

    def read_bytes(self, bytestring):
        d = json.loads(bytestring.decode('utf-8'))
        self.read_dict(d)
        return self

    def read_json(self, json_path):
        with open(json_path, 'r') as f:
            d = json.load(f)
        self.read_dict(d)
        return self

    def create_child(self, tag=None):
        """Creates and returns a follow up task object."""
        if tag is None:
            tag = self.tag + '__child'
        child_task = deepcopy(self)
        child_task.tag = tag
        child_task.update_timestamp = int(time.time())
        # make extra sure we don't route the task to the wrong site
        assert child_task.site_id == self.site_id
        return child_task

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return self.__str__()
