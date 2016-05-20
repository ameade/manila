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

import copy
import datetime

import ddt
import mock
import webob

from manila.api.v2 import group_types as types
from manila import exception
from manila import policy
from manila.share_group import group_types
from manila import test
from manila.tests.api import fakes

PROJ1_UUID = '11111111-1111-1111-1111-111111111111'
PROJ2_UUID = '22222222-2222-2222-2222-222222222222'
PROJ3_UUID = '33333333-3333-3333-3333-333333333333'

SHARE_TYPE_ID = '4b1e460f-8bc5-4a97-989b-739a2eceaec6'
GROUP_TYPE_1 = {
    'id': 'c8d7bf70-0db9-4b3e-8498-055dd0306461',
    'name': u'group type 1',
    'deleted': False,
    'created_at': datetime.datetime(2012, 1, 1, 1, 1, 1, 1),
    'updated_at': None,
    'deleted_at': None,
    'is_public': True,
    'extra_specs': {},
    'share_types': []
}

GROUP_TYPE_2 = {
    'id': 'f93f7a1f-62d7-4e7e-b9e6-72eec95a47f5',
    'name': u'group type 2',
    'deleted': False,
    'created_at': datetime.datetime(2012, 1, 1, 1, 1, 1, 1),
    'updated_at': None,
    'deleted_at': None,
    'is_public': False,
    'extra_specs': {'consistent_snapshots': 'true'},
    'share_types': [{'share_type_id': SHARE_TYPE_ID}]
}


def fake_request(url, admin=False, experimental=True, version='2.21',
                 **kwargs):

    return fakes.HTTPRequest.blank(url,
                                   use_admin_context=admin,
                                   experimental=experimental,
                                   version=version,
                                   **kwargs)


@ddt.ddt
class GroupTypesAPITest(test.TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.flags(host='fake')
        self.controller = types.ShareGroupTypesController()
        self.resource_name = self.controller.resource_name
        self.mock_object(policy, 'check_policy',
                         mock.Mock(return_value=True))

    def test_group_types_index(self):
        fake_types = {
            GROUP_TYPE_1['name']: GROUP_TYPE_1,
        }
        mock_get_all = self.mock_object(group_types, 'get_all_types',
                                        mock.Mock(return_value=fake_types))
        req = fake_request('/v2/fake/group-types', admin=False)
        expected_list = [{
            'id': GROUP_TYPE_1['id'],
            'name': GROUP_TYPE_1['name'],
            'is_public': True,
            'extra_specs': {},
            'share_types': [],
        }]

        res_dict = self.controller.index(req)

        mock_get_all.assert_called_once_with(mock.ANY,
                                             search_opts={"is_public": True})
        self.assertEqual(1, len(res_dict['group_types']))
        self.assertEqual(expected_list, res_dict['group_types'])

    def test_group_types_index_as_admin(self):
        fake_types = {
            GROUP_TYPE_1['name']: GROUP_TYPE_1,
            GROUP_TYPE_2['name']: GROUP_TYPE_2,
        }
        mock_get_all = self.mock_object(group_types, 'get_all_types',
                                        mock.Mock(return_value=fake_types))
        req = fake_request('/v2/fake/group-types?is_public=all', admin=True)
        expected_type_1 = {
            'id': GROUP_TYPE_1['id'],
            'name': GROUP_TYPE_1['name'],
            'is_public': True,
            'extra_specs': {},
            'share_types': [],
        }
        expected_type_2 = {
            'id': GROUP_TYPE_2['id'],
            'name': GROUP_TYPE_2['name'],
            'is_public': False,
            'extra_specs': {'consistent_snapshots': 'true'},
            'share_types': [SHARE_TYPE_ID],
        }

        res_dict = self.controller.index(req)

        mock_get_all.assert_called_once_with(mock.ANY,
                                             search_opts={'is_public': None})
        self.assertEqual(2, len(res_dict['group_types']))
        self.assertIn(expected_type_1, res_dict['group_types'])
        self.assertIn(expected_type_2, res_dict['group_types'])

    def test_group_types_index_as_admin_default_public_only(self):
        fake_types = {}
        mock_get_all = self.mock_object(group_types, 'get_all_types',
                                        mock.Mock(return_value=fake_types))
        req = fake_request('/v2/fake/group-types', admin=True)

        self.controller.index(req)

        mock_get_all.assert_called_once_with(mock.ANY,
                                             search_opts={'is_public': True})

    def test_group_types_index_not_experimental(self):
        self.mock_object(group_types, 'get_all_types',
                         mock.Mock(return_value={}))

        req = fake_request('/v2/fake/group-types', experimental=False)

        self.assertRaises(exception.VersionNotFoundForAPIMethod,
                          self.controller.index, req)

    def test_group_types_index_older_api_version(self):
        self.mock_object(group_types, 'get_all_types',
                         mock.Mock(return_value={}))

        req = fake_request('/v2/fake/group-types', version='2.1')

        self.assertRaises(exception.VersionNotFoundForAPIMethod,
                          self.controller.index, req)

    @ddt.data(True, False)
    def test_group_types_index_no_data(self, admin):
        self.mock_object(group_types, 'get_all_types',
                         mock.Mock(return_value={}))

        req = fake_request('/v2/fake/group-types', admin=admin)

        res_dict = self.controller.index(req)

        self.assertEqual(0, len(res_dict['group_types']))

    def test_group_types_show(self):
        mock_get = self.mock_object(group_types, 'get_group_type',
                                    mock.Mock(return_value=GROUP_TYPE_1))
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_1['id'])
        expected_type = {
            'id': GROUP_TYPE_1['id'],
            'name': GROUP_TYPE_1['name'],
            'is_public': True,
            'extra_specs': {},
            'share_types': [],
        }

        res_dict = self.controller.show(req, GROUP_TYPE_1['id'])

        mock_get.assert_called_once_with(mock.ANY, GROUP_TYPE_1['id'])
        self.assertEqual(expected_type, res_dict['group_type'])

    def test_group_types_show_with_share_types(self):
        mock_get = self.mock_object(group_types, 'get_group_type',
                                    mock.Mock(return_value=GROUP_TYPE_2))

        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'])
        expected_type = {
            'id': GROUP_TYPE_2['id'],
            'name': GROUP_TYPE_2['name'],
            'is_public': False,
            'extra_specs': {'consistent_snapshots': 'true'},
            'share_types': [SHARE_TYPE_ID],
        }

        res_dict = self.controller.show(req, GROUP_TYPE_2['id'])

        mock_get.assert_called_once_with(mock.ANY, GROUP_TYPE_2['id'])
        self.assertEqual(expected_type, res_dict['group_type'])

    def test_group_types_show_not_found(self):
        mock_get = self.mock_object(
            group_types, 'get_group_type',
            mock.Mock(
                side_effect=exception.GroupTypeNotFound(group_type_id=id)))

        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'])

        self.assertRaises(webob.exc.HTTPNotFound, self.controller.show,
                          req, GROUP_TYPE_2['id'])
        mock_get.assert_called_once_with(mock.ANY, GROUP_TYPE_2['id'])

    def test_group_types_default(self):
        mock_get = self.mock_object(
            group_types, 'get_default_group_type',
            mock.Mock(return_value=GROUP_TYPE_2))

        req = fake_request('/v2/fake/group-types/default')
        expected_type = {
            'id': GROUP_TYPE_2['id'],
            'name': GROUP_TYPE_2['name'],
            'is_public': False,
            'extra_specs': {'consistent_snapshots': 'true'},
            'share_types': [SHARE_TYPE_ID],
        }

        res_dict = self.controller.default(req)

        mock_get.assert_called_once_with(mock.ANY)
        self.assertEqual(expected_type, res_dict['group_type'])

    def test_group_types_default_not_found(self):
        mock_get = self.mock_object(
            group_types, 'get_default_group_type',
            mock.Mock(return_value=None))

        req = fake_request('/v2/fake/group-types/default')

        self.assertRaises(webob.exc.HTTPNotFound, self.controller.default, req)
        mock_get.assert_called_once_with(mock.ANY)

    def test_group_types_delete(self):
        mock_get = self.mock_object(group_types, 'get_group_type',
                                    mock.Mock(return_value=GROUP_TYPE_1))
        mock_destroy = self.mock_object(group_types, 'destroy')

        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_1['id'])

        self.controller._delete(req, GROUP_TYPE_1['id'])

        mock_get.assert_called_once_with(mock.ANY, GROUP_TYPE_1['id'])
        mock_destroy.assert_called_once_with(mock.ANY, GROUP_TYPE_1['id'])

    def test_group_types_delete_not_found(self):
        mock_get = self.mock_object(
            group_types, 'get_group_type',
            mock.Mock(
                side_effect=exception.GroupTypeNotFound(
                    group_type_id=GROUP_TYPE_2['id'])))

        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'])

        self.assertRaises(webob.exc.HTTPNotFound, self.controller._delete,
                          req, GROUP_TYPE_2['id'])
        mock_get.assert_called_once_with(mock.ANY, GROUP_TYPE_2['id'])

    def test_create_minimal(self):
        fake_type = copy.deepcopy(GROUP_TYPE_1)
        fake_type['share_types'] = [{'share_type_id': SHARE_TYPE_ID}]
        mock_create = self.mock_object(group_types, 'create')
        mock_get = self.mock_object(group_types, 'get_group_type_by_name',
                                    mock.Mock(return_value=fake_type))

        req = fake_request('/v2/fake/group-types')
        fake_body = {'group_type': {
            'name': GROUP_TYPE_1['name'],
            'share_types': [SHARE_TYPE_ID],
        }}
        expected_type = {
            'id': GROUP_TYPE_1['id'],
            'name': GROUP_TYPE_1['name'],
            'is_public': True,
            'extra_specs': {},
            'share_types': [SHARE_TYPE_ID],
        }

        res_dict = self.controller._create(req, fake_body)

        mock_create.assert_called_once_with(
            mock.ANY, GROUP_TYPE_1['name'],
            [SHARE_TYPE_ID], {}, True)
        mock_get.assert_called_once_with(mock.ANY, GROUP_TYPE_1['name'])
        self.assertEqual(expected_type, res_dict['group_type'])

    def test_create_with_extra_specs(self):
        fake_extra_specs = {'my_fake_extra_spec': 'false'}
        fake_type = copy.deepcopy(GROUP_TYPE_1)
        fake_type['share_types'] = [{'share_type_id': SHARE_TYPE_ID}]
        fake_type['extra_specs'] = fake_extra_specs
        mock_create = self.mock_object(group_types, 'create')
        mock_get = self.mock_object(group_types, 'get_group_type_by_name',
                                    mock.Mock(return_value=fake_type))

        req = fake_request('/v2/fake/group-types')
        fake_body = {'group_type': {
            'name': GROUP_TYPE_1['name'],
            'share_types': [SHARE_TYPE_ID],
            'extra_specs': fake_extra_specs,
        }}
        expected_type = {
            'id': GROUP_TYPE_1['id'],
            'name': GROUP_TYPE_1['name'],
            'is_public': True,
            'extra_specs': fake_extra_specs,
            'share_types': [SHARE_TYPE_ID],
        }

        res_dict = self.controller._create(req, fake_body)

        mock_create.assert_called_once_with(mock.ANY, GROUP_TYPE_1['name'],
                                            [SHARE_TYPE_ID], fake_extra_specs,
                                            True)
        mock_get.assert_called_once_with(mock.ANY, GROUP_TYPE_1['name'])
        self.assertEqual(expected_type, res_dict['group_type'])

    def test_create_private_type(self):
        fake_type = copy.deepcopy(GROUP_TYPE_1)
        fake_type['share_types'] = [{'share_type_id': SHARE_TYPE_ID}]
        fake_type['is_public'] = False
        mock_create = self.mock_object(group_types, 'create')
        mock_get = self.mock_object(group_types, 'get_group_type_by_name',
                                    mock.Mock(return_value=fake_type))

        req = fake_request('/v2/fake/group-types')
        fake_body = {'group_type': {
            'name': GROUP_TYPE_1['name'],
            'share_types': [SHARE_TYPE_ID],
            'is_public': False
        }}
        expected_type = {
            'id': GROUP_TYPE_1['id'],
            'name': GROUP_TYPE_1['name'],
            'is_public': False,
            'extra_specs': {},
            'share_types': [SHARE_TYPE_ID],
        }

        res_dict = self.controller._create(req, fake_body)

        mock_create.assert_called_once_with(mock.ANY, GROUP_TYPE_1['name'],
                                            [SHARE_TYPE_ID], {}, False)
        mock_get.assert_called_once_with(mock.ANY, GROUP_TYPE_1['name'])
        self.assertEqual(expected_type, res_dict['group_type'])

    def test_create_invalid_request_duplicate_name(self):
        mock_create = self.mock_object(
            group_types, 'create',
            mock.Mock(
                side_effect=exception.GroupTypeExists(
                    id=GROUP_TYPE_1['name'])))

        req = fake_request('/v2/fake/group-types')
        fake_body = {'group_type': {
            'name': GROUP_TYPE_1['name'],
            'share_types': [SHARE_TYPE_ID],
        }}

        self.assertRaises(webob.exc.HTTPConflict, self.controller._create,
                          req, fake_body)

        mock_create.assert_called_once_with(
            mock.ANY, GROUP_TYPE_1['name'],
            [SHARE_TYPE_ID], {}, True)

    def test_create_invalid_request_missing_name(self):
        req = fake_request('/v2/fake/group-types')
        fake_body = {'group_type': {
            'share_types': [SHARE_TYPE_ID],
        }}

        self.assertRaises(webob.exc.HTTPBadRequest, self.controller._create,
                          req, fake_body)

    def test_create_invalid_request_missing_share_types(self):
        req = fake_request('/v2/fake/group-types')
        fake_body = {'group_type': {
            'name': GROUP_TYPE_1['name'],
        }}

        self.assertRaises(webob.exc.HTTPBadRequest, self.controller._create,
                          req, fake_body)


class GroupTypeAccessTest(test.TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.controller = types.ShareGroupTypesController()

    def test_list_type_access_public(self):
        """Querying access on public type should return 404."""
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(return_value=GROUP_TYPE_1))

        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_1['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.group_type_access, req,
                          GROUP_TYPE_1['id'])

    def test_list_type_access_private(self):
        fake_type = copy.deepcopy(GROUP_TYPE_2)
        fake_type['projects'] = [PROJ2_UUID, PROJ3_UUID]
        mock_get = self.mock_object(group_types, 'get_group_type',
                                    mock.Mock(return_value=fake_type))
        expected = {'group_type_access': [
            {'group_type_id': fake_type['id'], 'project_id': PROJ2_UUID},
            {'group_type_id': fake_type['id'], 'project_id': PROJ3_UUID},
        ]}

        req = fake_request('/v2/fake/group-types/%s' % fake_type['id'],
                           admin=True)

        actual = self.controller.group_type_access(req, fake_type['id'])

        mock_get.assert_called_once_with(mock.ANY, fake_type['id'],
                                         expected_fields=['projects'])
        self.assertEqual(expected, actual)

    def test_list_type_access_type_not_found(self):
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(side_effect=exception.GroupTypeNotFound(
                             group_type_id=GROUP_TYPE_2['id'])))

        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.group_type_access, req,
                          GROUP_TYPE_2['id'])

    def test_add_project_access(self):
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(return_value=GROUP_TYPE_2))
        mock_add_access = self.mock_object(group_types,
                                           'add_group_type_access')
        body = {'addProjectAccess': {'project': PROJ1_UUID}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        response = self.controller._add_project_access(req, GROUP_TYPE_2['id'],
                                                       body)

        mock_add_access.assert_called_once_with(mock.ANY, GROUP_TYPE_2['id'],
                                                PROJ1_UUID)
        self.assertEqual(200, response.status_code)

    def test_add_project_access_non_existent_type(self):
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(side_effect=exception.GroupTypeNotFound(
                             group_type_id=GROUP_TYPE_2['id'])))
        body = {'addProjectAccess': {'project': PROJ1_UUID}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller._add_project_access, req,
                          GROUP_TYPE_2['id'], body)

    def test_add_project_access_missing_project_in_body(self):
        body = {'addProjectAccess': {}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller._add_project_access, req,
                          GROUP_TYPE_2['id'], body)

    def test_add_project_access_missing_add_project_access_in_body(self):
        body = {}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller._add_project_access, req,
                          GROUP_TYPE_2['id'], body)

    def test_add_project_access_with_already_added_access(self):
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(return_value=GROUP_TYPE_2))

        mock_add_access = self.mock_object(
            group_types, 'add_group_type_access',
            mock.Mock(side_effect=exception.GroupTypeAccessExists(
                group_type_id=GROUP_TYPE_2['id'], project_id=PROJ1_UUID)))
        body = {'addProjectAccess': {'project': PROJ1_UUID}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPConflict,
                          self.controller._add_project_access, req,
                          GROUP_TYPE_2['id'], body)
        mock_add_access.assert_called_once_with(mock.ANY, GROUP_TYPE_2['id'],
                                                PROJ1_UUID)

    def test_add_project_access_to_public_share_type(self):
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(return_value=GROUP_TYPE_1))

        body = {'addProjectAccess': {'project': PROJ1_UUID}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_1['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPConflict,
                          self.controller._add_project_access, req,
                          GROUP_TYPE_1['id'], body)

    def test_remove_project_access(self):
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(return_value=GROUP_TYPE_2))

        mock_remove_access = self.mock_object(group_types,
                                              'remove_group_type_access')
        body = {'removeProjectAccess': {'project': PROJ1_UUID}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        response = self.controller._remove_project_access(req,
                                                          GROUP_TYPE_2['id'],
                                                          body)

        mock_remove_access.assert_called_once_with(mock.ANY,
                                                   GROUP_TYPE_2['id'],
                                                   PROJ1_UUID)
        self.assertEqual(200, response.status_code)

    def test_remove_project_access_nonexistent_rule(self):
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(return_value=GROUP_TYPE_2))

        mock_remove_access = self.mock_object(
            group_types, 'remove_group_type_access', mock.Mock(
                side_effect=exception.GroupTypeAccessNotFound(
                    group_type_id=GROUP_TYPE_2['id'], project_id=PROJ1_UUID)))
        body = {'removeProjectAccess': {'project': PROJ1_UUID}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller._remove_project_access, req,
                          GROUP_TYPE_2['id'], body)
        mock_remove_access.assert_called_once_with(mock.ANY,
                                                   GROUP_TYPE_2['id'],
                                                   PROJ1_UUID)

    def test_remove_project_access_from_public_share_type(self):
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(return_value=GROUP_TYPE_1))

        body = {'removeProjectAccess': {'project': PROJ1_UUID}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_1['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPConflict,
                          self.controller._remove_project_access, req,
                          GROUP_TYPE_1['id'], body)

    def test_remove_project_access_non_existent_type(self):
        self.mock_object(group_types, 'get_group_type',
                         mock.Mock(side_effect=exception.GroupTypeNotFound(
                             group_type_id=GROUP_TYPE_2['id'])))
        body = {'removeProjectAccess': {'project': PROJ1_UUID}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller._remove_project_access, req,
                          GROUP_TYPE_2['id'], body)

    def test_remove_project_access_missing_project_in_body(self):
        body = {'removeProjectAccess': {}}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller._remove_project_access, req,
                          GROUP_TYPE_2['id'], body)

    def test_remove_project_access_missing_remove_project_access_in_body(self):
        body = {}
        req = fake_request('/v2/fake/group-types/%s' % GROUP_TYPE_2['id'],
                           admin=True)

        self.assertRaises(webob.exc.HTTPBadRequest,
                          self.controller._remove_project_access, req,
                          GROUP_TYPE_2['id'], body)
