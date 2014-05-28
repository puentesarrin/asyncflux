# -*- coding: utf-8 -*-
"""Tools for cluster administration"""
from asyncflux.common import BaseUsers


class ClusterAdmins(BaseUsers):

    def _get_path(self):
        return '/cluster_admins'

    def __repr__(self):
        return 'ClusterAdmins(%r)' % self.client
