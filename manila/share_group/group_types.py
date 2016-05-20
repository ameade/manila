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

"""Built-in group type properties."""

import re

from oslo_config import cfg
from oslo_db import exception as db_exception
from oslo_log import log
from oslo_utils import strutils
from oslo_utils import uuidutils
import six

from manila.common import constants
from manila import context
from manila import db
from manila import exception
from manila.i18n import _
from manila.i18n import _LE

CONF = cfg.CONF
LOG = log.getLogger(__name__)


def create(context, name, share_types, extra_specs=None, is_public=True,
           projects=None):
    """Creates group types."""
    extra_specs = extra_specs or {}
    projects = projects or []

    try:
        type_ref = db.group_type_create(context,
                                        dict(name=name,
                                             extra_specs=extra_specs,
                                             is_public=is_public,
                                             share_types=share_types),
                                        projects=projects)
    except db_exception.DBError:
        LOG.exception(_LE('DB error'))
        raise exception.GroupTypeCreateFailed(name=name,
                                              extra_specs=extra_specs)
    return type_ref


def destroy(context, id):
    """Marks group types as deleted."""
    if id is None:
        msg = _("id cannot be None")
        raise exception.InvalidShareGroupType(reason=msg)
    else:
        db.group_type_destroy(context, id)


def get_all_types(context, inactive=0, search_opts=None):
    """Get all non-deleted group_types.

    { types name: args}
    """
    # TODO(ameade): Fix docstring
    search_opts = search_opts or {}
    filters = {}

    if 'is_public' in search_opts:
        filters['is_public'] = search_opts.pop('is_public')

    group_types = db.group_type_get_all(context, inactive, filters=filters)

    if search_opts:
        LOG.debug("Searching by: %s", search_opts)

        def _check_extra_specs_match(group_type, searchdict):
            for k, v in searchdict.items():
                if (k not in group_type['extra_specs'].keys()
                        or group_type['extra_specs'][k] != v):
                    return False
            return True

        # search_option to filter_name mapping.
        filter_mapping = {'extra_specs': _check_extra_specs_match}

        result = {}
        for type_name, type_args in group_types.items():
            # go over all filters in the list
            for opt, values in search_opts.items():
                try:
                    filter_func = filter_mapping[opt]
                except KeyError:
                    # no such filter - ignore it, go to next filter
                    continue
                else:
                    if filter_func(type_args, values):
                        result[type_name] = type_args
                        break
        group_types = result
    return group_types


def get_group_type(ctxt, id, expected_fields=None):
    """Retrieves single group type by id."""
    if id is None:
        msg = _("id cannot be None")
        raise exception.InvalidShareGroupType(reason=msg)

    if ctxt is None:
        ctxt = context.get_admin_context()

    return db.group_type_get(ctxt, id, expected_fields=expected_fields)


def get_group_type_by_name(context, name):
    """Retrieves single group type by name."""
    if name is None:
        msg = _("name cannot be None")
        raise exception.InvalidShareGroupType(reason=msg)

    return db.group_type_get_by_name(context, name)


def get_group_type_by_name_or_id(context, group_type=None):
    if not group_type:
        group_type_ref = get_default_group_type(context)
        if not group_type_ref:
            msg = _("Default group type not found")
            raise exception.GroupTypeNotFound(reason=msg)
        return group_type_ref

    if uuidutils.is_uuid_like(group_type):
        return get_group_type(context, group_type)
    else:
        return get_group_type_by_name(context, group_type)


def get_default_group_type(ctxt=None):
    """Get the default group type."""
    name = CONF.default_group_type

    if name is None:
        return {}

    if ctxt is None:
        ctxt = context.get_admin_context()

    try:
        return get_group_type_by_name(ctxt, name)
    except exception.GroupTypeNotFoundByName:
        LOG.exception(_LE('Default group type is not found, '
                          'please check default_group_type config'))


def get_group_type_extra_specs(group_type_id, key=False):
    group_type = get_group_type(context.get_admin_context(),
                                group_type_id)
    extra_specs = group_type['extra_specs']
    if key:
        if extra_specs.get(key):
            return extra_specs.get(key)
        else:
            return False
    else:
        return extra_specs


def get_tenant_visible_extra_specs():
    return constants.ExtraSpecs.TENANT_VISIBLE


def get_boolean_extra_specs():
    return constants.ExtraSpecs.BOOLEAN


def add_group_type_access(context, group_type_id, project_id):
    """Add access to group type for project_id."""
    if group_type_id is None:
        msg = _("group_type_id cannot be None")
        raise exception.InvalidShareGroupType(reason=msg)
    return db.group_type_access_add(context, group_type_id, project_id)


def remove_group_type_access(context, group_type_id, project_id):
    """Remove access to group type for project_id."""
    if group_type_id is None:
        msg = _("group_type_id cannot be None")
        raise exception.InvalidShareGroupType(reason=msg)
    return db.group_type_access_remove(context, group_type_id, project_id)


def group_types_diff(context, group_type_id1, group_type_id2):
    """Returns a 'diff' of two group types and whether they are equal.

    Returns a tuple of (diff, equal), where 'equal' is a boolean indicating
    whether there is any difference, and 'diff' is a dictionary with the
    following format:
    {'extra_specs': {
    'key1': (value_in_1st_group_type, value_in_2nd_group_type),
    'key2': (value_in_1st_group_type, value_in_2nd_group_type),
    ...}
    """

    def _dict_diff(dict1, dict2):
        res = {}
        equal = True
        if dict1 is None:
            dict1 = {}
        if dict2 is None:
            dict2 = {}
        for k, v in dict1.items():
            res[k] = (v, dict2.get(k))
            if k not in dict2 or res[k][0] != res[k][1]:
                equal = False
        for k, v in dict2.items():
            res[k] = (dict1.get(k), v)
            if k not in dict1 or res[k][0] != res[k][1]:
                equal = False
        return (res, equal)

    all_equal = True
    diff = {}
    group_type1 = get_group_type(context, group_type_id1)
    group_type2 = get_group_type(context, group_type_id2)

    extra_specs1 = group_type1.get('extra_specs')
    extra_specs2 = group_type2.get('extra_specs')
    diff['extra_specs'], equal = _dict_diff(extra_specs1, extra_specs2)
    if not equal:
        all_equal = False

    return (diff, all_equal)


def get_extra_specs_from_group(group):
    type_id = group.get('group_type_id', None)
    return get_group_type_extra_specs(type_id)


def parse_boolean_extra_spec(extra_spec_key, extra_spec_value):
    """Parse extra spec values of the form '<is> True' or '<is> False'

    This method returns the boolean value of an extra spec value.  If
    the value does not conform to the standard boolean pattern, it raises
    an InvalidExtraSpec exception.
    """

    try:
        if not isinstance(extra_spec_value, six.string_types):
            raise ValueError

        match = re.match(r'^<is>\s*(?P<value>True|False)$',
                         extra_spec_value.strip(),
                         re.IGNORECASE)
        if not match:
            raise ValueError
        else:
            return strutils.bool_from_string(match.group('value'), strict=True)
    except ValueError:
        msg = (_('Invalid boolean extra spec %(key)s : %(value)s') %
               {'key': extra_spec_key, 'value': extra_spec_value})
        raise exception.InvalidExtraSpec(reason=msg)
