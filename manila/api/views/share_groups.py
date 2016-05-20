# Copyright 2015 Alex Meade
# All Rights Reserved.
#
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

from manila.api import common


class GroupViewBuilder(common.ViewBuilder):
    """Model a share group API response as a python dictionary."""

    _collection_name = 'groups'

    def summary_list(self, request, groups):
        """Show a list of share groups without many details."""
        return self._list_view(self.summary, request, groups)

    def detail_list(self, request, groups):
        """Detailed view of a list of share groups."""
        return self._list_view(self.detail, request, groups)

    def summary(self, request, group):
        """Generic, non-detailed view of a share group."""
        return {
            'group': {
                'id': group.get('id'),
                'name': group.get('name'),
                'links': self._get_links(request, group['id'])
            }
        }

    def detail(self, request, group):
        """Detailed view of a single share group."""
        context = request.environ['manila.context']
        group_dict = {
            'id': group.get('id'),
            'name': group.get('name'),
            'created_at': group.get('created_at'),
            'status': group.get('status'),
            'description': group.get('description'),
            'project_id': group.get('project_id'),
            'host': group.get('host'),
            'group_type_id': group.get('share_group_type_id'),
            'source_group_snapshot_id': group.get('source_group_snapshot_id'),
            'share_network_id': group.get('share_network_id'),
            'share_types': [st['share_type_id'] for st in group.get(
                'share_types')],
            'links': self._get_links(request, group['id']),
        }
        if context.is_admin:
            group_dict['share_server_id'] = group.get('share_server_id')
        return {'group': group_dict}

    def _list_view(self, func, request, shares):
        """Provide a view for a list of share groups."""
        group_list = [func(request, share)['group']
                      for share in shares]
        groups_links = self._get_collection_links(
            request, shares, self._collection_name)
        groups_dict = dict(groups=group_list)

        if groups_links:
            groups_dict['groups_links'] = groups_links

        return groups_dict
