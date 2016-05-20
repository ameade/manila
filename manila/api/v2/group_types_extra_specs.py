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

import copy
import six
import webob

from manila.api import common
from manila.api.openstack import wsgi
from manila import db
from manila import exception
from manila.i18n import _
from manila.share_group import group_types


class GroupTypeExtraSpecsController(wsgi.Controller):
    """The group type extra specs API controller for the OpenStack API."""

    resource_name = 'group_types_extra_spec'

    def _get_extra_specs(self, context, type_id):
        extra_specs = db.group_type_extra_specs_get(context, type_id)
        return dict(extra_specs=copy.deepcopy(extra_specs))

    def _assert_group_type_exists(self, context, type_id):
        try:
            group_types.get_group_type(context, type_id)
        except exception.NotFound as ex:
            raise webob.exc.HTTPNotFound(explanation=ex.msg)

    def _verify_extra_specs(self, extra_specs):

        def is_valid_string(v):
            return isinstance(v, six.string_types) and len(v) in range(1, 256)

        def is_valid_extra_spec(k, v):
            valid_extra_spec_key = is_valid_string(k)
            valid_type = is_valid_string(v) or isinstance(v, bool)
            return valid_extra_spec_key and valid_type

        for k, v in extra_specs.items():
            if is_valid_string(k) and isinstance(v, dict):
                self._verify_extra_specs(v)
            elif not is_valid_extra_spec(k, v):
                expl = _('Invalid extra_spec: %(key)s: %(value)s') % {
                    'key': k, 'value': v
                }
                raise webob.exc.HTTPBadRequest(explanation=expl)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def index(self, req, type_id):
        """Returns the list of extra specs for a given group type."""

        context = req.environ['manila.context']
        self._assert_group_type_exists(context, type_id)
        return self._get_extra_specs(context, type_id)

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def create(self, req, type_id, body=None):
        context = req.environ['manila.context']

        if not self.is_valid_body(body, 'extra_specs'):
            raise webob.exc.HTTPBadRequest()

        self._assert_group_type_exists(context, type_id)
        specs = body['extra_specs']
        self._verify_extra_specs(specs)
        self._check_key_names(specs.keys())
        db.group_type_extra_specs_update_or_create(context, type_id, specs)
        return body

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def update(self, req, type_id, key, body=None):
        context = req.environ['manila.context']
        if not body:
            expl = _('Request body empty')
            raise webob.exc.HTTPBadRequest(explanation=expl)
        self._assert_group_type_exists(context, type_id)
        if key not in body:
            expl = _('Request body and URI mismatch')
            raise webob.exc.HTTPBadRequest(explanation=expl)
        if len(body) > 1:
            expl = _('Request body contains too many items')
            raise webob.exc.HTTPBadRequest(explanation=expl)
        self._verify_extra_specs(body)
        db.group_type_extra_specs_update_or_create(context, type_id, body)
        return body

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def show(self, req, type_id, key):
        """Return a single extra spec item."""
        context = req.environ['manila.context']
        self._assert_group_type_exists(context, type_id)
        specs = self._get_extra_specs(context, type_id)
        if key in specs['extra_specs']:
            return {key: specs['extra_specs'][key]}
        else:
            raise webob.exc.HTTPNotFound()

    @wsgi.Controller.api_version('2.21', experimental=True)
    @wsgi.Controller.authorize
    def delete(self, req, type_id, key):
        """Deletes an existing extra spec."""
        context = req.environ['manila.context']
        self._assert_group_type_exists(context, type_id)

        try:
            db.group_type_extra_specs_delete(context, type_id, key)
        except exception.GroupTypeExtraSpecsNotFound as error:
            raise webob.exc.HTTPNotFound(explanation=error.msg)

        return webob.Response(status_int=204)

    def _check_key_names(self, keys):
        if not common.validate_key_names(keys):
            expl = _('Key names can only contain alphanumeric characters, '
                     'underscores, periods, colons and hyphens.')

            raise webob.exc.HTTPBadRequest(explanation=expl)


def create_resource():
    return wsgi.Resource(GroupTypeExtraSpecsController())
