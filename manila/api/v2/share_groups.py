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

"""The share groups API."""

from oslo_log import log
from oslo_utils import uuidutils
import six
import webob
from webob import exc

from manila.api import common
from manila.api.openstack import wsgi
import manila.api.views.share_groups as group_views
from manila import db
from manila import exception
from manila.i18n import _
from manila.i18n import _LI
from manila.share import share_types
import manila.share_group.api as group_api
from manila.share_group import group_types


LOG = log.getLogger(__name__)


class GroupController(wsgi.Controller, wsgi.AdminActionsMixin):
    """The Share Groups API controller for the OpenStack API."""

    resource_name = 'group'
    _view_builder_class = group_views.GroupViewBuilder

    def __init__(self):
        super(GroupController, self).__init__()
        self.group_api = group_api.API()

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize('get')
    def show(self, req, id):
        """Return data about the given group."""
        context = req.environ['manila.context']

        try:
            group = self.group_api.get(context, id)
        except exception.NotFound:
            msg = _("Share group %s not found.") % id
            raise exc.HTTPNotFound(explanation=msg)

        return self._view_builder.detail(req, group)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def delete(self, req, id):
        """Delete a group."""
        context = req.environ['manila.context']

        LOG.info(_LI("Delete share group with id: %s"), id, context=context)

        try:
            group = self.group_api.get(context, id)
        except exception.NotFound:
            msg = _("Share group %s not found.") % id
            raise exc.HTTPNotFound(explanation=msg)

        try:
            self.group_api.delete(context, group)
        except exception.InvalidShareGroup as e:
            raise exc.HTTPConflict(explanation=six.text_type(e))

        return webob.Response(status_int=202)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize('get_all')
    def index(self, req):
        """Returns a summary list of shares."""
        return self._get_groups(req, is_detail=False)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize('get_all')
    def detail(self, req):
        """Returns a detailed list of shares."""
        return self._get_groups(req, is_detail=True)

    def _get_groups(self, req, is_detail):
        """Returns a list of shares, transformed through view builder."""
        context = req.environ['manila.context']

        search_opts = {}
        search_opts.update(req.GET)

        # Remove keys that are not related to group attrs
        search_opts.pop('limit', None)
        search_opts.pop('offset', None)
        sort_key = search_opts.pop('sort_key', 'created_at')
        sort_dir = search_opts.pop('sort_dir', 'desc')
        if 'group_type_id' in search_opts:
            search_opts['share_group_type_id'] = search_opts.pop(
                'group_type_id')

        groups = self.group_api.get_all(
            context, detailed=is_detail, search_opts=search_opts,
            sort_dir=sort_dir, sort_key=sort_key
        )

        limited_list = common.limited(groups, req)

        if is_detail:
            groups = self._view_builder.detail_list(req, limited_list)
        else:
            groups = self._view_builder.summary_list(req, limited_list)
        return groups

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def update(self, req, id, body):
        """Update a share."""
        context = req.environ['manila.context']

        if not self.is_valid_body(body, 'group'):
            msg = _("'group' is missing from the request body.")
            raise exc.HTTPBadRequest(explanation=msg)

        group_data = body['group']
        valid_update_keys = {
            'name',
            'description',
        }
        invalid_fields = set(group_data.keys()) - valid_update_keys
        if invalid_fields:
            msg = _("The fields %s are invalid or not allowed to be updated.")
            raise exc.HTTPBadRequest(explanation=msg % invalid_fields)

        try:
            group = self.group_api.get(context, id)
        except exception.NotFound:
            msg = _("Share group %s not found.") % id
            raise exc.HTTPNotFound(explanation=msg)

        group = self.group_api.update(context, group, group_data)
        return self._view_builder.detail(req, group)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.response(202)
    @wsgi.Controller.authorize
    def create(self, req, body):
        """Creates a new share."""
        context = req.environ['manila.context']

        if not self.is_valid_body(body, 'group'):
            msg = _("'group' is missing from the request body.")
            raise exc.HTTPBadRequest(explanation=msg)

        group = body['group']

        valid_fields = {'name', 'description', 'share_types',
                        'group_type_id', 'source_group_snapshot_id',
                        'share_network_id'}
        invalid_fields = set(group.keys()) - valid_fields
        if invalid_fields:
            msg = _("The fields %s are invalid.") % invalid_fields
            raise exc.HTTPBadRequest(explanation=msg)

        if 'share_types' in group and 'source_group_snapshot_id' in group:
            msg = _("Cannot supply both 'share_types' and "
                    "'source_group_snapshot_id' attributes.")
            raise exc.HTTPBadRequest(explanation=msg)

        if (not group.get('share_types') and
                'source_group_snapshot_id' not in group):
            default_share_type = share_types.get_default_share_type()
            if default_share_type:
                group['share_types'] = [default_share_type['id']]
            else:
                msg = _("Must specify at least one share type as a default "
                        "share type has not been configured.")
                raise exc.HTTPBadRequest(explanation=msg)

        kwargs = {}

        if 'name' in group:
            kwargs['name'] = group.get('name')
        if 'description' in group:
            kwargs['description'] = group.get('description')

        _share_types = group.get('share_types')
        if _share_types:
            if not all([uuidutils.is_uuid_like(st) for st in _share_types]):
                msg = _("The 'share_types' attribute must be a list of uuids")
                raise exc.HTTPBadRequest(explanation=msg)
            kwargs['share_type_ids'] = _share_types

        if 'share_network_id' in group and 'source_group_snapshot_id' in group:
            msg = _("Cannot supply both 'share_network_id' and "
                    "'source_group_snapshot_id' attributes as the share "
                    "network is inherited from the source.")
            raise exc.HTTPBadRequest(explanation=msg)

        if 'source_group_snapshot_id' in group:
            source_group_snapshot_id = group.get('source_group_snapshot_id')
            if not uuidutils.is_uuid_like(source_group_snapshot_id):
                msg = _("The 'source_group_snapshot_id' attribute must be a "
                        "uuid.")
                raise exc.HTTPBadRequest(explanation=six.text_type(msg))
            kwargs['source_group_snapshot_id'] = source_group_snapshot_id

        elif 'share_network_id' in group:
            share_network_id = group.get('share_network_id')
            if not uuidutils.is_uuid_like(share_network_id):
                msg = _("The 'share_network_id' attribute must be a uuid.")
                raise exc.HTTPBadRequest(explanation=six.text_type(msg))
            kwargs['share_network_id'] = share_network_id

        if 'group_type_id' in group:
            share_group_type_id = group.get('group_type_id')
            if not uuidutils.is_uuid_like(share_group_type_id):
                msg = _("The 'group_type_id' attribute must be a uuid.")
                raise exc.HTTPBadRequest(explanation=six.text_type(msg))
            kwargs['share_group_type_id'] = share_group_type_id
        else:  # get default
            def_group_type = group_types.get_default_group_type()
            if def_group_type:
                kwargs['share_group_type_id'] = def_group_type['id']
            else:
                msg = _("Must specify a share group type as a default "
                        "share group type has not been configured.")
                raise exc.HTTPBadRequest(explanation=msg)

        try:
            new_group = self.group_api.create(context, **kwargs)
        except exception.InvalidGroupSnapshot as e:
            raise exc.HTTPConflict(explanation=six.text_type(e))
        except (exception.GroupSnapshotNotFound, exception.InvalidInput) as e:
            raise exc.HTTPBadRequest(explanation=six.text_type(e))

        return self._view_builder.detail(req, dict(new_group.items()))

    def _update(self, *args, **kwargs):
        db.share_group_update(*args, **kwargs)

    def _get(self, *args, **kwargs):
        return self.group_api.get(*args, **kwargs)

    def _delete(self, context, resource, force=True):
        # TODO(ameade): force delete all of the shares in the group

        db.share_group_destroy(context.elevated(), resource['id'])

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action('reset_status')
    def group_reset_status(self, req, id, body):
        return self._reset_status(req, id, body)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action('force_delete')
    def group_force_delete(self, req, id, body):
        return self._force_delete(req, id, body)


def create_resource():
    return wsgi.Resource(GroupController())
