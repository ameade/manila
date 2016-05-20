#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""The share groups snapshot API."""

from oslo_log import log
from oslo_utils import uuidutils
import six
import webob
from webob import exc

from manila.api import common
from manila.api.openstack import wsgi
import manila.api.views.group_snapshots as group_views
from manila import db
from manila import exception
from manila.i18n import _
from manila.i18n import _LI
import manila.share_group.api as group_api

LOG = log.getLogger(__name__)


class GroupSnapshotController(wsgi.Controller, wsgi.AdminActionsMixin):
    """The share group snapshots API controller for the OpenStack API."""

    resource_name = 'group_snapshot'
    _view_builder_class = group_views.GroupSnapshotViewBuilder

    def __init__(self):
        super(GroupSnapshotController, self).__init__()
        self.group_api = group_api.API()

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize('get_group_snapshot')
    def show(self, req, id):
        """Return data about the given group snapshot."""
        context = req.environ['manila.context']

        try:
            group = self.group_api.get_group_snapshot(context, id)
        except exception.NotFound:
            msg = _("share group snapshot %s not found.") % id
            raise exc.HTTPNotFound(explanation=msg)

        return self._view_builder.detail(req, group)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def delete(self, req, id):
        """Delete a group snapshot."""
        context = req.environ['manila.context']

        LOG.info(_LI("Delete share group snapshot with id: %s"), id,
                 context=context)

        try:
            snap = self.group_api.get_group_snapshot(context, id)
        except exception.NotFound:
            msg = _("share group snapshot %s not found.") % id
            raise exc.HTTPNotFound(explanation=msg)

        try:
            self.group_api.delete_group_snapshot(context, snap)
        except exception.InvalidGroupSnapshot as e:
            raise exc.HTTPConflict(explanation=six.text_type(e))

        return webob.Response(status_int=202)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize('get_all')
    def index(self, req):
        """Returns a summary list of group snapshots."""
        return self._get_group_snaps(req, is_detail=False)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize('get_all')
    def detail(self, req):
        """Returns a detailed list of group snapshots."""
        return self._get_group_snaps(req, is_detail=True)

    def _get_group_snaps(self, req, is_detail):
        """Returns a list of group snapshots."""
        context = req.environ['manila.context']

        search_opts = {}
        search_opts.update(req.GET)

        # Remove keys that are not related to group attrs
        search_opts.pop('limit', None)
        search_opts.pop('offset', None)
        sort_key = search_opts.pop('sort_key', 'created_at')
        sort_dir = search_opts.pop('sort_dir', 'desc')

        snaps = self.group_api.get_all_group_snapshots(
            context, detailed=is_detail, search_opts=search_opts,
            sort_dir=sort_dir, sort_key=sort_key)

        limited_list = common.limited(snaps, req)

        if is_detail:
            snaps = self._view_builder.detail_list(req, limited_list)
        else:
            snaps = self._view_builder.summary_list(req, limited_list)
        return snaps

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def update(self, req, id, body):
        """Update a group snapshot."""
        context = req.environ['manila.context']

        if not self.is_valid_body(body, 'group_snapshot'):
            msg = _("'group_snapshot' is missing from the request body")
            raise exc.HTTPBadRequest(explanation=msg)

        group_data = body['group_snapshot']
        valid_update_keys = {
            'name',
            'description',
        }
        invalid_fields = set(group_data.keys()) - valid_update_keys
        if invalid_fields:
            msg = _("The fields %s are invalid or not allowed to be updated.")
            raise exc.HTTPBadRequest(explanation=msg % invalid_fields)

        try:
            group = self.group_api.get_group_snapshot(context, id)
        except exception.NotFound:
            msg = _("share group snapshot %s not found.") % id
            raise exc.HTTPNotFound(explanation=msg)

        group = self.group_api.update_group_snapshot(
            context, group, group_data)
        return self._view_builder.detail(req, group)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.response(202)
    @wsgi.Controller.authorize
    def create(self, req, body):
        """Creates a new group snapshot."""
        context = req.environ['manila.context']

        if not self.is_valid_body(body, 'group_snapshot'):
            msg = _("'group_snapshot' is missing from the request body")
            raise exc.HTTPBadRequest(explanation=msg)

        group_snapshot = body.get('group_snapshot')

        if not group_snapshot.get('share_group_id'):
            msg = _("Must supply 'share_group_id' attribute.")
            raise exc.HTTPBadRequest(explanation=msg)

        share_group_id = group_snapshot.get('share_group_id')
        if (share_group_id and
                not uuidutils.is_uuid_like(share_group_id)):
            msg = _("The 'share_group_id' attribute must be a uuid.")
            raise exc.HTTPBadRequest(explanation=six.text_type(msg))

        kwargs = {"share_group_id": share_group_id}

        if 'name' in group_snapshot:
            kwargs['name'] = group_snapshot.get('name')
        if 'description' in group_snapshot:
            kwargs['description'] = group_snapshot.get('description')

        try:
            new_snapshot = self.group_api.create_group_snapshot(context,
                                                                **kwargs)
        except exception.ShareGroupNotFound as e:
            raise exc.HTTPBadRequest(explanation=six.text_type(e))
        except exception.InvalidShareGroup as e:
            raise exc.HTTPConflict(explanation=six.text_type(e))

        return self._view_builder.detail(req, dict(new_snapshot.items()))

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize('get_group_snapshot')
    def members(self, req, id):
        """Returns a list of group_snapshot members."""
        context = req.environ['manila.context']

        search_opts = {}
        search_opts.update(req.GET)

        # Remove keys that are not related to group attrs
        search_opts.pop('limit', None)
        search_opts.pop('offset', None)

        snaps = self.group_api.get_all_group_snapshot_members(context, id)

        limited_list = common.limited(snaps, req)

        snaps = self._view_builder.member_list(req, limited_list)
        return snaps

    def _update(self, *args, **kwargs):
        db.group_snapshot_update(*args, **kwargs)

    def _get(self, *args, **kwargs):
        return self.group_api.get_group_snapshot(*args, **kwargs)

    def _delete(self, context, resource, force=True):
        db.group_snapshot_destroy(context.elevated(), resource['id'])

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action('os-reset_status')
    def group_snapshot_reset_status_legacy(self, req, id, body):
        return self._reset_status(req, id, body)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action('reset_status')
    def group_snapshot_reset_status(self, req, id, body):
        return self._reset_status(req, id, body)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action('os-force_delete')
    def group_snapshot_force_delete_legacy(self, req, id, body):
        return self._force_delete(req, id, body)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action('force_delete')
    def group_snapshot_force_delete(self, req, id, body):
        return self._force_delete(req, id, body)


def create_resource():
    return wsgi.Resource(GroupSnapshotController())
