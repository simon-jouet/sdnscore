from ryu.topology import event
from ryu.controller import ofp_event
from ryu.base import app_manager
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.topology.api import get_all_switch, get_switch, get_link

import json
from webob import Response, exc
from ryu.app.wsgi import ControllerBase, WSGIApplication, route

from contextlib import closing

from ryu.sdnmgmt import routing
import networkx as nx

import logging

LOG = logging.getLogger(__name__)


class MACPair(object):
    def __init__(self, mac1, mac2):
        if mac1 > mac2:
            mac1, mac2 = mac2, mac1

        self.mac1 = mac1
        self.mac2 = mac2

    def __str__(self):
        return '{} <> {}'.format(self.mac1, self.mac2)

    def __hash__(self):
        return hash((self.mac1, self.mac2))

    def __cmp__(self, other):
        return self.mac1 == other.mac1 and self.mac2 == other.mac2

    def __eq__(self, other):
        return self.__cmp__(other)

    def to_dict(self):
        return self.__dict__


class SDNMgmt(app_manager.RyuApp):
    _CONTEXTS = {
        'wsgi': WSGIApplication,
        'routing': routing.Routing
    }

    def __init__(self, *args, **kwargs):
        super(SDNMgmt, self).__init__(*args, **kwargs)

        self.routing = kwargs['routing']

        wsgi = kwargs['wsgi']
        wsgi.register(SDNMgmtController, {'topology_api_app': self})

        self.stats = {}


    def send_flow_stats_request(self, datapath):
        ofp = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        req = ofp_parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev):
        switch_stats = {}
        # print 'FLOW STAT REPLY'

        # Iterate over all the flow stats
        # print ev.msg.body
        for stat in ev.msg.body:
            eth_src = stat.match.get('eth_src')
            eth_dst = stat.match.get('eth_dst')

            # If flow contains both source and destination
            if eth_src and eth_dst:
                key = MACPair(eth_src, eth_dst)

                # list of matching flows, should only be two
                switch_stats.setdefault(key, []).append(stat)

        #
        for key,v in switch_stats.items():
            if len(v) != 2:
                LOG.debug('unexpected number of flow stats, should only get forward and return path')
                return

            # Get byte_count both ways and duration in seconds
            byte_count = v[0].byte_count + v[1].byte_count
            duration = v[0].duration_sec + v[0].duration_nsec / 10.0**9

            last_byte_count = 0
            last_duration = 0

            if key in self.stats:
                last_byte_count = self.stats[key]['last_byte_count']
                last_duration = self.stats[key]['last_duration']

                # If previous byte count is larger, then the counter overflows or flow timed out
                # if last_byte_count > byte_count:
                    # only consider overflow, time out considered below
                    # byte count is 64bits
                    # LOG.debug('counter overflowed')
                    # (byte_count + 2**64) - last_byte_count

                # If previous flow duration is larger then the flow timeout in between the measurements
                # if last_duration > duration:
                    # print 'timedout', last_duration, duration
                    # last_duration = 0
                    # last_byte_count = 0

                # Ignore if this switch is not responsible for this flow
                if self.stats[key]['dpid'] != ev.msg.datapath.id:
                    LOG.debug('switch {} is not responsible for this flow {} is'.format(ev.msg.datapath.id, self.stats[key]['dpid']))
                    continue

            # Calculate traffic_rate over time period
            delta_bytes = byte_count - last_byte_count
            delta_duration = duration - last_duration
            traffic_rate = delta_bytes / delta_duration

            # print v[0].hard_timeout, v[0].idle_timeout
            # print 'delta byte count', str(key), last_byte_count, byte_count, delta_bytes, last_duration, duration, delta_duration

            # Save the stats
            self.stats[key] = {
                'delta_bytes':     delta_bytes,
                'delta_duration':  delta_duration,
                'traffic_rate':    traffic_rate,
                'dpid':            ev.msg.datapath.id,
                'last_byte_count': byte_count,
                'last_duration':   duration,
            }


class SDNMgmtController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(SDNMgmtController, self).__init__(req, link, data, **config)
        self.topology_api_app = data['topology_api_app']

    @route('sdnmgmt', '/v1.0/sdnmgmt/query', methods=['POST'])
    def query_flowstats(self, req, **kwargs):
        for dpid in self.topology_api_app.routing.hypervisor_mac_to_dpid.values():
            # print 'querying', dpid
            switch = self.topology_api_app.routing.topology.node[dpid]['obj']
            self.topology_api_app.send_flow_stats_request(switch.dp)

        return Response(content_type='application/json', body='')

    @route('sdnmgmt', '/v1.0/sdnmgmt/view', methods=['GET'])
    def view_flowstats(self, req, **kwargs):
        res = {}
        for k,v in self.topology_api_app.stats.items():
            key = str(k)
            res[key] = v
            res[key]['endpoints'] = k.to_dict()

        return Response(content_type='application/json', body=json.dumps(res))

    @route('sdnmgmt', '/v1.0/sdnmgmt/placement', methods=['GET'])
    def placement(self, req, **kwargs):
        res = {}
        for n,d in self.topology_api_app.routing.topology.nodes_iter(data=True):
            if d.get('type') == 'vm':
                res[n] = self.topology_api_app.routing.get_hypervisor(n)

        return Response(content_type='application/json', body=json.dumps(res))

    @route('sdnmgmt', '/v1.0/sdnmgmt/macs', methods=['GET'])
    def view_macs(self, req, **kwargs):
        return Response(content_type='application/json', body=json.dumps(self.topology_api_app.routing.mac_to_ip))


    @route('sdnmgmt', '/v1.0/sdnmgmt/cost', methods=['GET'])
    def calculate_path_cost(self, req, **kwargs):
        src = req.params.get('src')
        dst = req.params.get('dst')

        if not src and not dst:
            return exc.HTTPBadRequest()
        elif src and not dst:
            body = json.dumps({ hypervisor: self.topology_api_app.routing.calculate_path_cost(src, hypervisor) for hypervisor in self.topology_api_app.routing.hypervisor_mac_to_dpid.values() })
            return Response(content_type='application/json', body=body)
        else:
            body = json.dumps(self.topology_api_app.routing.calculate_path_cost(src, dst))
            return Response(content_type='application/json', body=body)

    @route('sdnmgmt', '/v1.0/sdnmgmt/discovery', methods=['GET'])
    def discovery(self, req, **kwargs):
        dst = req.params.get('dst')

        if not dst:
            return exc.HTTPBadRequest()

        for d in dst.split(','):
            self.topology_api_app.routing.discover_host(d)

    @route('sdnmgmt', '/v1.0/sdnmgmt/hypervisors', methods=['GET'])
    def hypervisors(self, req, **kwargs):
        return Response(content_type='application/json', body=json.dumps(self.topology_api_app.routing.hypervisor_mac_to_dpid))

    @route('sdnmgmt', '/v1.0/sdnmgmt/remove', methods=['POST'])
    def remove(self, req, **kwargs):
        src = req.params.get('src')
        dst = req.params.get('dst')

        if src and dst:
            key = routing.SrcDestMACPair(src, dst)
            path = self.topology_api_app.routing.installed_paths[key]
            self.topology_api_app.routing.uninstall_path(src, dst, path)

            return Response(content_type='application/json', body=json.dumps(path))
        else:
            return exc.HTTPBadRequest()

    @route('sdnmgmt', '/v1.0/sdnmgmt/migrate', methods=['POST'])
    def migrate(self, req, **kwargs):
        mac = req.params.get('mac')

        if not mac:
            return exc.HTTPBadRequest()

        self.topology_api_app.routing.migrate(mac)