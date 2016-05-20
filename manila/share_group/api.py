# Copyright (c) 2015 Alex Meade
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

"""
Handles all requests relating to consistency groups.
"""

from oslo_config import cfg
from oslo_log import log
from oslo_utils import excutils
from oslo_utils import strutils
import six

from manila.common import constants
from manila.db import base
from manila import exception
from manila.i18n import _
from manila.scheduler import rpcapi as scheduler_rpcapi
from manila import share
from manila.share import rpcapi as share_rpcapi
from manila.share import share_types


CONF = cfg.CONF

LOG = log.getLogger(__name__)


class API(base.Base):
    """API for interacting with the share manager."""

    def __init__(self, db_driver=None):
        self.scheduler_rpcapi = scheduler_rpcapi.SchedulerAPI()
        self.share_rpcapi = share_rpcapi.ShareAPI()
        self.share_api = share.API()
        super(API, self).__init__(db_driver)

    def create(self, context, name=None, description=None,
               share_type_ids=None, source_group_snapshot_id=None,
               share_network_id=None, share_group_type_id=None):
        """Create new consistency group."""

        group_snapshot = None
        original_group = None
        # NOTE(gouthamr): share_server_id is inherited from the parent group
        # if a  group snapshot is specified, else, it will be set in the
        # share manager.
        share_server_id = None
        if source_group_snapshot_id:
            group_snapshot = self.db.group_snapshot_get(
                context, source_group_snapshot_id)
            if group_snapshot['status'] != constants.STATUS_AVAILABLE:
                msg = (_("Share group snapshot status must be %s")
                       % constants.STATUS_AVAILABLE)
                raise exception.InvalidGroupSnapshot(reason=msg)

            original_group = self.db.share_group_get(context, group_snapshot[
                'share_group_id'])
            share_type_ids = [s['share_type_id'] for s in original_group[
                'share_types']]
            share_network_id = original_group['share_network_id']
            share_server_id = original_group['share_server_id']

        # Get share_type_objects
        share_type_objects = []
        driver_handles_share_servers = None
        for share_type_id in (share_type_ids or []):
            try:
                share_type_object = share_types.get_share_type(
                    context, share_type_id)
            except exception.ShareTypeNotFound:
                msg = _("Share type with id %s could not be found")
                raise exception.InvalidInput(msg % share_type_id)
            share_type_objects.append(share_type_object)

            extra_specs = share_type_object.get('extra_specs')
            if extra_specs:
                share_type_handle_ss = strutils.bool_from_string(
                    extra_specs.get(
                        constants.ExtraSpecs.DRIVER_HANDLES_SHARE_SERVERS))
                if driver_handles_share_servers is None:
                    driver_handles_share_servers = share_type_handle_ss
                elif not driver_handles_share_servers == share_type_handle_ss:
                    # NOTE(ameade): if the share types have conflicting values
                    #  for driver_handles_share_servers then raise bad request
                    msg = _("The specified share_types cannot have "
                            "conflicting values for the "
                            "driver_handles_share_servers extra spec.")
                    raise exception.InvalidInput(reason=msg)

                if (not share_type_handle_ss) and share_network_id:
                    msg = _("When using a share types with the "
                            "driver_handles_share_servers extra spec as "
                            "False, a share_network_id must not be provided.")
                    raise exception.InvalidInput(reason=msg)

        try:
            if share_network_id:
                self.db.share_network_get(context, share_network_id)
        except exception.ShareNetworkNotFound:
            msg = _("The specified share network does not exist.")
            raise exception.InvalidInput(reason=msg)

        if (driver_handles_share_servers and
                not (source_group_snapshot_id or share_network_id)):
            msg = _("When using a share type with the "
                    "driver_handles_share_servers extra spec as "
                    "True, a share_network_id must be provided.")
            raise exception.InvalidInput(reason=msg)

        try:
            group_type = self.db.group_type_get(context, share_group_type_id)
        except exception.ShareGroupTypeNotFound:
            msg = _("The specified share group type does not exist.")
            raise exception.InvalidInput(reason=msg)

        supported_share_types = set([x['share_type_id']
                                     for x in group_type['share_types']])

        if not set(share_type_ids or []) <= supported_share_types:
            msg = _("The specified share types must be a subset of the share "
                    "types supported by the share group type.")
            raise exception.InvalidInput(reason=msg)

        options = {
            'share_group_type_id': share_group_type_id,
            'source_group_snapshot_id': source_group_snapshot_id,
            'share_network_id': share_network_id,
            'share_server_id': share_server_id,
            'name': name,
            'description': description,
            'user_id': context.user_id,
            'project_id': context.project_id,
            'status': constants.STATUS_CREATING,
            'share_types': share_type_ids or supported_share_types
        }
        if original_group:
            options['host'] = original_group['host']

        group = self.db.share_group_create(context, options)

        try:
            if group_snapshot:
                members = self.db.group_snapshot_members_get_all(
                    context, source_group_snapshot_id)
                for member in members:
                    share = self.db.share_get(context, member['share_id'])
                    share_type = share_types.get_share_type(
                        context, share['share_type_id'])
                    member['share_instance'] = self.db.share_instance_get(
                        context, member['share_instance_id'],
                        with_share_data=True)
                    self.share_api.create(context, member['share_proto'],
                                          member['size'], None, None,
                                          share_group_id=group['id'],
                                          group_snapshot_member=member,
                                          share_type=share_type,
                                          share_network_id=share_network_id)
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.share_group_destroy(context.elevated(), group['id'])

        request_spec = {'share_group_id': group['id']}
        request_spec.update(options)
        request_spec['share_types'] = share_type_objects
        request_spec['resource_type'] = group_type

        if group_snapshot and original_group:
            self.share_rpcapi.create_share_group(
                context, group, original_group['host'])
        else:
            self.scheduler_rpcapi.create_share_group(
                context, group_id=group['id'], request_spec=request_spec,
                filter_properties={})

        return group

    def delete(self, context, group):
        """Delete consistency group."""

        group_id = group['id']
        if not group['host']:
            self.db.share_group_destroy(context.elevated(), group_id)
            return

        statuses = (constants.STATUS_AVAILABLE, constants.STATUS_ERROR)
        if not group['status'] in statuses:
            msg = (_("Consistency group status must be one of %(statuses)s")
                   % {"statuses": statuses})
            raise exception.InvalidShareGroup(reason=msg)

        # NOTE(ameade): check for group_snapshots in the group
        if self.db.count_group_snapshots_in_share_group(context, group_id):
            msg = (_("Cannot delete a consistency group with group_snapshots"))
            raise exception.InvalidShareGroup(reason=msg)

        # NOTE(ameade): check for shares in the group
        if self.db.count_shares_in_share_group(context, group_id):
            msg = (_("Cannot delete a consistency group with shares"))
            raise exception.InvalidShareGroup(reason=msg)

        group = self.db.share_group_update(
            context, group_id, {'status': constants.STATUS_DELETING})

        self.share_rpcapi.delete_share_group(context, group)

    def update(self, context, group, fields):
        return self.db.share_group_update(context, group['id'], fields)

    def get(self, context, group_id):
        return self.db.share_group_get(context, group_id)

    def get_all(self, context, detailed=True, search_opts=None, sort_key=None,
                sort_dir=None):

        if search_opts is None:
            search_opts = {}

        LOG.debug("Searching for share_groups by: %s",
                  six.text_type(search_opts))

        # Get filtered list of share_groups
        if context.is_admin and search_opts.get('all_tenants'):
            share_groups = self.db.share_group_get_all(
                context, detailed=detailed, filters=search_opts,
                sort_key=sort_key, sort_dir=sort_dir)
        else:
            share_groups = self.db.share_group_get_all_by_project(
                context, context.project_id, detailed=detailed,
                filters=search_opts, sort_key=sort_key, sort_dir=sort_dir)

        return share_groups

    def create_group_snapshot(self, context, name=None, description=None,
                              share_group_id=None):
        """Create new group_snapshot."""

        options = {
            'share_group_id': share_group_id,
            'name': name,
            'description': description,
            'user_id': context.user_id,
            'project_id': context.project_id,
            'status': constants.STATUS_CREATING,
        }

        group = self.db.share_group_get(context, share_group_id)
        # Check status of group, must be active
        if not group['status'] == constants.STATUS_AVAILABLE:
            msg = (_("Share group status must be %s")
                   % constants.STATUS_AVAILABLE)
            raise exception.InvalidShareGroup(reason=msg)

        # Create members for every share in the group
        shares = self.db.share_get_all_by_share_group_id(
            context, share_group_id)

        # Check status of all shares, they must be active in order to snap
        # the group
        for s in shares:
            if not s['status'] == constants.STATUS_AVAILABLE:
                msg = (_("Share %(s)s in share group must have status "
                         "of %(status)s in order to create a group snapshot")
                       % {"s": s['id'],
                          "status": constants.STATUS_AVAILABLE})
                raise exception.InvalidShareGroup(reason=msg)

        snap = self.db.group_snapshot_create(context, options)

        try:
            members = []
            for s in shares:
                member_options = {
                    'group_snapshot_id': snap['id'],
                    'user_id': context.user_id,
                    'project_id': context.project_id,
                    'status': constants.STATUS_CREATING,
                    'size': s['size'],
                    'share_proto': s['share_proto'],
                    'share_id': s['id'],
                    'share_instance_id': s.instance['id']
                }
                member = self.db.group_snapshot_member_create(
                    context, member_options)
                members.append(member)

            # Cast to share manager
            self.share_rpcapi.create_group_snapshot(context, snap,
                                                    group['host'])
        except Exception:
            with excutils.save_and_reraise_exception():
                # This will delete the snapshot and all of it's members
                self.db.group_snapshot_destroy(context, snap['id'])

        return snap

    def delete_group_snapshot(self, context, snap):
        """Delete consistency group snapshot."""

        snap_id = snap['id']

        group = self.db.share_group_get(context,
                                        snap['share_group_id'])

        statuses = (constants.STATUS_AVAILABLE, constants.STATUS_ERROR)
        if not snap['status'] in statuses:
            msg = (_("Consistency group snapshot status must be one of"
                     " %(statuses)s")
                   % {"statuses": statuses})
            raise exception.InvalidGroupSnapshot(reason=msg)

        self.db.group_snapshot_update(context, snap_id,
                                      {'status': constants.STATUS_DELETING})

        # Cast to share manager
        self.share_rpcapi.delete_group_snapshot(context, snap, group['host'])

    def update_group_snapshot(self, context, group, fields):
        return self.db.group_snapshot_update(context, group['id'], fields)

    def get_group_snapshot(self, context, snapshot_id):
        return self.db.group_snapshot_get(context, snapshot_id)

    def get_all_group_snapshots(self, context, detailed=True,
                                search_opts=None, sort_key=None,
                                sort_dir=None):

        if search_opts is None:
            search_opts = {}

        LOG.debug("Searching for consistency group snapshots by: %s",
                  six.text_type(search_opts))

        # Get filtered list of share_groups
        if context.is_admin and search_opts.get('all_tenants'):
            group_snapshots = self.db.group_snapshot_get_all(
                context, detailed=detailed, filters=search_opts,
                sort_key=sort_key, sort_dir=sort_dir)
        else:
            group_snapshots = self.db.group_snapshot_get_all_by_project(
                context, context.project_id, detailed=detailed,
                filters=search_opts, sort_key=sort_key, sort_dir=sort_dir)

        return group_snapshots

    def get_all_group_snapshot_members(self, context, group_snapshot_id):
        members = self.db.group_snapshot_members_get_all(
            context,  group_snapshot_id)

        return members
