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


"""Test of Share Type methods for Manila."""
import copy
import datetime

import ddt
import mock

from manila.common import constants
from manila import context
from manila import db
from manila import exception
from manila.share_group import group_types
from manila import test


def create_group_type_dict(extra_specs=None):
    return {
        'fake_type': {
            'name': 'fake1',
            'extra_specs': extra_specs
        }
    }


@ddt.ddt
class GroupTypesTestCase(test.TestCase):

    fake_type = {
        'test': {
            'created_at': datetime.datetime(2015, 1, 22, 11, 43, 24),
            'deleted': '0',
            'deleted_at': None,
            'extra_specs': {},
            'id': u'fooid-1',
            'name': u'test',
            'updated_at': None
        }
    }
    fake_extra_specs = {u'gold': u'True'}
    fake_group_type_id = u'fooid-2'
    fake_type_w_extra = {
        'test_with_extra': {
            'created_at': datetime.datetime(2015, 1, 22, 11, 45, 31),
            'deleted': '0',
            'deleted_at': None,
            'extra_specs': fake_extra_specs,
            'id': fake_group_type_id,
            'name': u'test_with_extra',
            'updated_at': None
        }
    }

    fake_type_w_valid_extra = {
        'test_with_extra': {
            'created_at': datetime.datetime(2015, 1, 22, 11, 45, 31),
            'deleted': '0',
            'deleted_at': None,
            'extra_specs': {
                constants.ExtraSpecs.DRIVER_HANDLES_SHARE_SERVERS: 'true'
            },
            'id': u'fooid-2',
            'name': u'test_with_extra',
            'updated_at': None
        }
    }

    fake_types = fake_type.copy()
    fake_types.update(fake_type_w_extra)
    fake_types.update(fake_type_w_valid_extra)

    fake_group = {'id': u'fooid-1', 'group_type_id': fake_group_type_id}

    def setUp(self):
        super(GroupTypesTestCase, self).setUp()
        self.context = context.get_admin_context()

    @ddt.data({}, fake_type, fake_type_w_extra, fake_types)
    def test_get_all_types(self, group_type):
        self.mock_object(db,
                         'group_type_get_all',
                         mock.Mock(return_value=copy.deepcopy(group_type)))
        returned_type = group_types.get_all_types(self.context)
        self.assertItemsEqual(group_type, returned_type)

    def test_get_all_types_search(self):
        group_type = self.fake_type_w_extra
        search_filter = {"extra_specs": {"gold": "True"}, 'is_public': True}
        self.mock_object(db,
                         'group_type_get_all',
                         mock.Mock(return_value=group_type))
        returned_type = group_types.get_all_types(self.context,
                                                  search_opts=search_filter)
        db.group_type_get_all.assert_called_once_with(
            mock.ANY, 0, filters={'is_public': True})
        self.assertItemsEqual(group_type, returned_type)
        search_filter = {"extra_specs": {"gold": "False"}}
        returned_type = group_types.get_all_types(self.context,
                                                  search_opts=search_filter)
        self.assertEqual({}, returned_type)

    def test_get_group_type_extra_specs(self):
        group_type = self.fake_type_w_extra['test_with_extra']
        self.mock_object(db,
                         'group_type_get',
                         mock.Mock(return_value=group_type))
        id = group_type['id']
        extra_spec = group_types.get_group_type_extra_specs(id, key='gold')
        self.assertEqual(group_type['extra_specs']['gold'], extra_spec)
        extra_spec = group_types.get_group_type_extra_specs(id)
        self.assertEqual(group_type['extra_specs'], extra_spec)

    def test_group_types_diff(self):
        group_type1 = self.fake_type['test']
        group_type2 = self.fake_type_w_extra['test_with_extra']
        expeted_diff = {'extra_specs': {u'gold': (None, u'True')}}
        self.mock_object(db,
                         'group_type_get',
                         mock.Mock(side_effect=[group_type1, group_type2]))
        (diff, equal) = group_types.group_types_diff(self.context,
                                                     group_type1['id'],
                                                     group_type2['id'])
        self.assertFalse(equal)
        self.assertEqual(expeted_diff, diff)

    def test_group_types_diff_equal(self):
        group_type = self.fake_type['test']
        self.mock_object(db,
                         'group_type_get',
                         mock.Mock(return_value=group_type))
        (diff, equal) = group_types.group_types_diff(self.context,
                                                     group_type['id'],
                                                     group_type['id'])
        self.assertTrue(equal)

    def test_get_extra_specs_from_group(self):
        expected = self.fake_extra_specs
        self.mock_object(group_types, 'get_group_type_extra_specs',
                         mock.Mock(return_value=expected))

        spec_value = group_types.get_extra_specs_from_group(self.fake_group)

        self.assertEqual(expected, spec_value)
        group_types.get_group_type_extra_specs.assert_called_once_with(
            self.fake_group_type_id)

    def test_add_access(self):
        project_id = '456'
        group_type = group_types.create(self.context, 'type1', [])
        group_type_id = group_type.get('id')

        group_types.add_group_type_access(self.context, group_type_id,
                                          project_id)
        stype_access = db.group_type_access_get_all(self.context,
                                                    group_type_id)
        self.assertIn(project_id, [a.project_id for a in stype_access])

    def test_add_access_invalid(self):
        self.assertRaises(exception.InvalidShareGroupType,
                          group_types.add_group_type_access,
                          'fake', None, 'fake')

    def test_remove_access(self):
        project_id = '456'
        group_type = group_types.create(
            self.context, 'type1', [], projects=['456'])
        group_type_id = group_type.get('id')

        group_types.remove_group_type_access(self.context, group_type_id,
                                             project_id)
        stype_access = db.group_type_access_get_all(self.context,
                                                    group_type_id)
        self.assertNotIn(project_id, stype_access)

    def test_remove_access_invalid(self):
        self.assertRaises(exception.InvalidShareGroupType,
                          group_types.remove_group_type_access,
                          'fake', None, 'fake')

    @ddt.data({'spec_value': '<is> True', 'expected': True},
              {'spec_value': '<is>true', 'expected': True},
              {'spec_value': '<is> False', 'expected': False},
              {'spec_value': '<is>false', 'expected': False},
              {'spec_value': u' <is> FaLsE ', 'expected': False})
    @ddt.unpack
    def test_parse_boolean_extra_spec(self, spec_value, expected):

        result = group_types.parse_boolean_extra_spec('fake_key', spec_value)

        self.assertEqual(expected, result)

    @ddt.data('True', 'False', '<isnt> True', '<is> Wrong', None, 5)
    def test_parse_boolean_extra_spec_invalid(self, spec_value):

        self.assertRaises(exception.InvalidExtraSpec,
                          group_types.parse_boolean_extra_spec,
                          'fake_key',
                          spec_value)
