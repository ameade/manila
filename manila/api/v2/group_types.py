# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""The group type API controller module."""

from oslo_utils import strutils
from oslo_utils import uuidutils
import six
import webob
from webob import exc

from manila.api.openstack import wsgi
from manila.api.views import group_types as views_types
from manila import exception
from manila.i18n import _
from manila.share_group import group_types


class ShareGroupTypesController(wsgi.Controller):
    """The group types API controller for the OpenStack API."""

    resource_name = 'share_group_type'
    _view_builder_class = views_types.ViewBuilder

    def __getattr__(self, key):
        return super(self.__class__, self).__getattr__(key)

    def _check_body(self, body, action_name):
        if not self.is_valid_body(body, action_name):
            raise webob.exc.HTTPBadRequest()
        access = body[action_name]
        project = access.get('project')
        if not uuidutils.is_uuid_like(project):
            msg = _("Project value (%s) must be in uuid format.") % project
            raise webob.exc.HTTPBadRequest(explanation=msg)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def index(self, req):
        """Returns the list of group types."""
        limited_types = self._get_group_types(req)
        return self._view_builder.index(req, limited_types)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def show(self, req, id):
        """Return a single group type item."""
        context = req.environ['manila.context']
        try:
            group_type = group_types.get_group_type(context, id)
        except exception.NotFound:
            msg = _("Group type not found.")
            raise exc.HTTPNotFound(explanation=msg)

        group_type['id'] = six.text_type(group_type['id'])
        return self._view_builder.show(req, group_type)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def default(self, req):
        """Return default group type."""
        context = req.environ['manila.context']

        group_type = group_types.get_default_group_type(context)
        if not group_type:
            msg = _("Default group type not found")
            raise exc.HTTPNotFound(explanation=msg)

        group_type['id'] = six.text_type(group_type['id'])
        return self._view_builder.show(req, group_type)

    def _get_group_types(self, req):
        """Helper function that returns a list of group type dicts."""
        filters = {}
        context = req.environ['manila.context']
        if context.is_admin:
            # Only admin has query access to all group types
            filters['is_public'] = self._parse_is_public(
                req.params.get('is_public'))
        else:
            # TODO(ameade): Is this correct? can I still see private types
            # shared with me?
            filters['is_public'] = True
        limited_types = group_types.get_all_types(context,
                                                  search_opts=filters).values()
        return list(limited_types)

    @staticmethod
    def _parse_is_public(is_public):
        """Parse is_public into something usable.

        * True: API should list public group types only
        * False: API should list private group types only
        * None: API should list both public and private group types
        """
        if is_public is None:
            # preserve default value of showing only public types
            return True
        elif six.text_type(is_public).lower() == "all":
            return None
        else:
            try:
                return strutils.bool_from_string(is_public, strict=True)
            except ValueError:
                msg = _('Invalid is_public filter [%s]') % is_public
                raise exc.HTTPBadRequest(explanation=msg)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action("create")
    @wsgi.Controller.authorize('create')
    def _create(self, req, body):
        """Creates a new group type."""
        context = req.environ['manila.context']

        if not self.is_valid_body(body, 'group_type'):
            raise webob.exc.HTTPBadRequest()

        group_type = body['group_type']
        name = group_type.get('name')
        specs = group_type.get('extra_specs', {})
        is_public = group_type.get('is_public', True)

        if not group_type.get('share_types'):
            msg = _("Supported share types must be provided")
            raise webob.exc.HTTPBadRequest(explanation=msg)
        share_types = group_type.get('share_types')

        if name is None or name == "" or len(name) > 255:
            msg = _("Type name is not valid.")
            raise webob.exc.HTTPBadRequest(explanation=msg)

        try:
            group_types.create(context, name, share_types, specs, is_public)
            group_type = group_types.get_group_type_by_name(context, name)
        except exception.GroupTypeExists as err:
            raise webob.exc.HTTPConflict(explanation=six.text_type(err))
        except exception.NotFound:
            raise webob.exc.HTTPNotFound()

        return self._view_builder.show(req, group_type)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action("delete")
    @wsgi.Controller.authorize('delete')
    def _delete(self, req, id):
        """Deletes an existing group type."""
        context = req.environ['manila.context']

        try:
            group_type = group_types.get_group_type(context, id)
            group_types.destroy(context, group_type['id'])
        except exception.GroupTypeInUse:
            msg = _('Target group type is still in use.')
            raise webob.exc.HTTPBadRequest(explanation=msg)
        except exception.NotFound:
            raise webob.exc.HTTPNotFound()

        return webob.Response(status_int=204)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize('list_project_access')
    def group_type_access(self, req, id):
        context = req.environ['manila.context']

        try:
            group_type = group_types.get_group_type(
                context, id, expected_fields=['projects'])
        except exception.GroupTypeNotFound:
            explanation = _("Group type %s not found.") % id
            raise webob.exc.HTTPNotFound(explanation=explanation)

        if group_type['is_public']:
            expl = _("Access list not available for public group types.")
            raise webob.exc.HTTPNotFound(explanation=expl)

        projects = []
        for project_id in group_type['projects']:
            projects.append(
                {'group_type_id': group_type['id'], 'project_id': project_id}
            )
        return {'group_type_access': projects}

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action('addProjectAccess')
    @wsgi.Controller.authorize('add_project_access')
    def _add_project_access(self, req, id, body):
        context = req.environ['manila.context']
        self._check_body(body, 'addProjectAccess')
        project = body['addProjectAccess']['project']

        self._assert_non_public_group_type(context, id)

        try:
            group_types.add_group_type_access(context, id, project)
        except exception.GroupTypeAccessExists as err:
            raise webob.exc.HTTPConflict(explanation=six.text_type(err))

        return webob.Response(status_int=200)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.action('removeProjectAccess')
    @wsgi.Controller.authorize('remove_project_access')
    def _remove_project_access(self, req, id, body):
        context = req.environ['manila.context']
        self._check_body(body, 'removeProjectAccess')
        project = body['removeProjectAccess']['project']

        self._assert_non_public_group_type(context, id)

        try:
            group_types.remove_group_type_access(context, id, project)
        except exception.GroupTypeAccessNotFound as err:
            raise webob.exc.HTTPNotFound(explanation=six.text_type(err))
        return webob.Response(status_int=200)

    def _assert_non_public_group_type(self, context, group_type_id):
        try:
            group_type = group_types.get_group_type(context, group_type_id)

            if group_type['is_public']:
                msg = _("Type access modification is not applicable to "
                        "public group type.")
                raise webob.exc.HTTPConflict(explanation=msg)

        except exception.GroupTypeNotFound as err:
            raise webob.exc.HTTPNotFound(explanation=six.text_type(err))


def create_resource():
    return wsgi.Resource(ShareGroupTypesController())
