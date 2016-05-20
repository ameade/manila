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

"""The share groups snapshot API."""

from manila.api import common


class GroupSnapshotViewBuilder(common.ViewBuilder):
    """Model a group snapshot API response as a python dictionary."""

    _collection_name = 'group_snapshot'

    def summary_list(self, request, group_snaps):
        """Show a list of group_snapshots without many details."""
        return self._list_view(self.summary, request, group_snaps)

    def detail_list(self, request, group_snaps):
        """Detailed view of a list of group_snapshots."""
        return self._list_view(self.detail, request, group_snaps)

    def member_list(self, request, members):
        members_list = []
        for member in members:
            member_dict = {
                'id': member.get('id'),
                'created_at': member.get('created_at'),
                'size': member.get('size'),
                'share_protocol': member.get('share_proto'),
                'project_id': member.get('project_id'),
                'group_snapshot_id': member.get('group_snapshot_id'),
                'share_id': member.get('share_id'),
            }
            members_list.append(member_dict)

        members_links = self._get_collection_links(request,
                                                   members,
                                                   'group_snapshot_id')
        members_dict = dict(group_snapshot_members=members_list)

        if members_links:
            members_dict['group_snapshot_members_links'] = members_links

        return members_dict

    def summary(self, request, group_snap):
        """Generic, non-detailed view of a group snapshot."""
        return {
            'group_snapshot': {
                'id': group_snap.get('id'),
                'name': group_snap.get('name'),
                'links': self._get_links(request, group_snap['id'])
            }
        }

    def detail(self, request, group_snap):
        """Detailed view of a single group snapshot."""

        members = self._format_member_list(
            group_snap.get('group_snapshot_members', []))

        group_snap_dict = {
            'id': group_snap.get('id'),
            'name': group_snap.get('name'),
            'created_at': group_snap.get('created_at'),
            'status': group_snap.get('status'),
            'description': group_snap.get('description'),
            'project_id': group_snap.get('project_id'),
            'share_group_id': group_snap.get('share_group_id'),
            'members': members,
            'links': self._get_links(request, group_snap['id']),
        }
        return {'group_snapshot': group_snap_dict}

    def _format_member_list(self, members):
        members_list = []
        for member in members:
            member_dict = {
                'id': member.get('id'),
                'size': member.get('size'),
                'share_id': member.get('share_id'),
            }
            members_list.append(member_dict)

        return members_list

    def _list_view(self, func, request, snaps):
        """Provide a view for a list of group snapshots."""
        snap_list = [func(request, snap)['group_snapshot']
                     for snap in snaps]
        snaps_links = self._get_collection_links(request,
                                                 snaps,
                                                 self._collection_name)
        snaps_dict = dict(group_snapshots=snap_list)

        if snaps_links:
            snaps_dict['group_snapshot_links'] = snaps_links

        return snaps_dict
