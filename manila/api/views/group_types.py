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

from manila.api import common


class ViewBuilder(common.ViewBuilder):

    _collection_name = 'types'

    def show(self, request, group_type, brief=False):
        """Trim away extraneous group type attributes."""

        extra_specs = group_type.get('extra_specs', {})

        trimmed = {
            'id': group_type.get('id'),
            'name': group_type.get('name'),
            'is_public': group_type.get('is_public'),
            'extra_specs': extra_specs,
            'share_types': [st['share_type_id']
                            for st in group_type['share_types']],
        }
        self.update_versioned_resource_dict(request, trimmed, group_type)
        if brief:
            return trimmed
        else:
            return dict(group_type=trimmed)

    def index(self, request, group_types):
        """Index over trimmed share types."""
        group_types_list = [self.show(request, group_type, True)
                            for group_type in group_types]
        return dict(group_types=group_types_list)
