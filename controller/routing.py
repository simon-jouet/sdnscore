import logging

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3, ether
from ryu.lib.packet import arp, ethernet, ipv4, packet, lldp
from ryu.topology import event
from ryu.topology.switches import Link, Port
from ryu.lib import mac

import networkx as nx
import array

LOG = logging.getLogger(__name__)

CONTROLLER_MAC = '02:02:02:02:02:02'

class NetworkPort(object):
    def __init__(self, id, port):
        self.id = id
        self.port = port

class NetworkLink(object):
    def __init__(self, src, dst):
        self.src = src
        self.dst = dst

    def getPortById(self, id):
        if self.src.id == id:
            return self.src
        elif self.dst.id == id:
            return self.dst
        else:
            return None

class SrcDestMACPair(object):
    def __init__(self, src, dst):
        self.src = src
        self.dst = dst

    def __hash__(self):
        return hash((self.src, self.dst))

    def __cmp__(self, other):
        return self.src == other.src and self.dst == other.dst

    def __eq__(self, other):
        return self.__cmp__(other)

    def __str__(self):
        return '{} - {}'.format(self.src, self.dst)

def dpid_to_str(dpid):
    return '{:016x}'.format(dpid)

class Routing(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Routing, self).__init__(*args, **kwargs)
        self.name = 'routing'

        self.mac_to_ip = {}
        self.ip_to_mac = {}
        self.topology = nx.Graph()

        self.hypervisor_mac_to_dpid = { '90:b1:1c:87:72:c5': "0000000000000004" } 
        self.hypervisor_dpid_to_mac = { v: k for k, v in self.hypervisor_mac_to_dpid.items() }

        self.installed_paths = {}

    """ Triggered when the switch is being configure, make sure to redirect ARP packets that are relevant """
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch(eth_type=ether.ETH_TYPE_ARP)
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]

        # Redirect all the ARP packets to the controller
        self.add_flow(datapath, 1, match, actions)

        # Don't want to redirect ARP request packets from the controller to the controller
        match = parser.OFPMatch(eth_type=ether.ETH_TYPE_ARP, eth_src=CONTROLLER_MAC)
        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        self.add_flow(datapath, 1, match, actions)

    @set_ev_cls(event.EventSwitchEnter)
    def _switch_enter_handler(self, ev):
        print 'SWITCH enter', dpid_to_str(ev.switch.dp.id)
        self.topology.add_node(dpid_to_str(ev.switch.dp.id), type='switch', obj=ev.switch)

    @set_ev_cls(event.EventSwitchLeave)
    def _switch_leave_handler(self, ev):
        print 'SWITCH DISCONNECT', dpid_to_str(ev.switch.dp.id)
        self.topology.remove_node(dpid_to_str(ev.switch.dp.id))


    @set_ev_cls(event.EventLinkAdd)
    def _link_add_handler(self, ev):
        print 'LINK ADD', ev.link
        self.topology.add_edge(dpid_to_str(ev.link.src.dpid), dpid_to_str(ev.link.dst.dpid), type='link', obj=NetworkLink(NetworkPort(dpid_to_str(ev.link.src.dpid), ev.link.src.port_no), NetworkPort(dpid_to_str(ev.link.dst.dpid), ev.link.dst.port_no)))

    @set_ev_cls(event.EventLinkDelete)
    def _link_del_handler(self, ev):
        print 'LINK DELETE NYI', ev.link

    def add_host(self, datapath, in_port, src_mac, src_ip):
        ofproto = datapath.ofproto

        if (src_mac not in self.mac_to_ip) and (src_mac != CONTROLLER_MAC) and src_ip != '0.0.0.0':
            print 'adding entry:', src_mac, src_ip, in_port, dpid_to_str(datapath.id)
            self.mac_to_ip[src_mac] = src_ip
            self.ip_to_mac[src_ip]  = src_mac

            if in_port == ofproto.OFPP_LOCAL:
                print 'local port, this is the hypervisor !', src_mac
                self.hypervisor_mac_to_dpid[src_mac] = dpid_to_str(datapath.id)
                self.hypervisor_dpid_to_mac[dpid_to_str(datapath.id)] = src_mac

                host_type = 'hypervisor'
            else:
                print 'adding VM', src_mac
                host_type = 'vm'

            self.topology.add_node(src_mac, type=host_type)
            self.topology.add_edge(src_mac, dpid_to_str(datapath.id), type='link', obj=NetworkLink(NetworkPort(src_mac, None), NetworkPort(dpid_to_str(datapath.id), in_port)))

    def remove_host(self, src_mac):
        src_ip = self.mac_to_ip.pop(src_mac, None)
        if src_ip:
            self.ip_to_mac.pop(src_ip, None)

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst)

        datapath.send_msg(mod)

    def remove_flow(self, datapath, match):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        mod = parser.OFPFlowMod(datapath=datapath, match=match, command=ofproto.OFPFC_DELETE, out_port=ofproto.OFPP_ANY, out_group=ofproto.OFPG_ANY)
        datapath.send_msg(mod)

    def get_path(self, src, dst):
        path = nx.shortest_path(self.topology, source=src, target=dst)
        return path

    def install_path(self, src, dst):
        print 'Installing path from {} to {}'.format(src, dst)
        path = self.get_path(src, dst)
        print path

        ### Install route between source and destination
        #  Check if the path hasn't been installed already, otherwise
        #  it installs a flow that already exists and therefore resets
        #  the counters
        installed_path = self.installed_paths.get(SrcDestMACPair(src, dst))
        if not installed_path:
            for i in range(1, len(path)-1): # Iterate only over the switches
                switch_dpid = path[i]
                switch_edges = self.topology[switch_dpid]
                switch = self.topology.node[switch_dpid]['obj']

                ingress = switch_edges[path[i-1]]['obj']
                egress  = switch_edges[path[i+1]]['obj']

                ingress_port = ingress.getPortById(switch_dpid).port
                egress_port = egress.getPortById(switch_dpid).port

                parser = switch.dp.ofproto_parser

                self.add_flow(switch.dp, 0, parser.OFPMatch(in_port=ingress_port, eth_src=src, eth_dst=dst), [parser.OFPActionOutput(egress_port)])
                self.add_flow(switch.dp, 0, parser.OFPMatch(in_port=egress_port, eth_src=dst, eth_dst=src), [parser.OFPActionOutput(ingress_port)])


            self.installed_paths[SrcDestMACPair(src, dst)] = path
            self.installed_paths[SrcDestMACPair(dst, src)] = path[::-1]
            print 'Path installed from {} to {}'.format(src, dst)

        return path

    def get_hypervisor(self, mac):
        if mac in self.topology:
            adjacencies = self.topology[mac]
            for n,e in adjacencies.items():
                if self.topology.node[n].get('type') == 'switch':
                    return n

        return None


    def calculate_path_cost(self, src, dst):
        # Link Cost
        if not self.topology.node.get(src) or not self.topology.node.get(dst):
            print '{} or {} not in the topology'.format(src, dst)
            return None

        path = nx.shortest_path(self.topology,
            source=src,
            target=dst
        )

        # print path
        nb_switches = len(path[1:-1]) # 1:-1 to remove source and dest
        nb_links = len(path)-1
        max_cost = nb_links / 2
        sum_cost = max_cost * (max_cost + 1)

        return {
            'switches': nb_switches,
            'links': nb_links,
            'max_cost': max_cost,
            'total_cost': sum_cost
        }

    def uninstall_path(self, src, dst, path):
        for i in range(1, len(path)-1): # Iterate only over the switches
            switch_dpid = path[i]
            switch_edges = self.topology[switch_dpid]
            switch = self.topology.node[switch_dpid]['obj']

            ingress = switch_edges[path[i-1]]['obj']
            egress  = switch_edges[path[i+1]]['obj']

            ingress_port = ingress.getPortById(switch_dpid).port
            egress_port = egress.getPortById(switch_dpid).port

            datapath = switch.dp
            parser = switch.dp.ofproto_parser
            self.remove_flow(datapath, parser.OFPMatch(in_port=ingress_port, eth_src=src, eth_dst=dst))
            self.remove_flow(datapath, parser.OFPMatch(in_port=egress_port, eth_src=dst, eth_dst=src))

            if SrcDestMACPair(src, dst) in self.installed_paths:
                del self.installed_paths[SrcDestMACPair(src, dst)]
            if SrcDestMACPair(dst, src) in self.installed_paths:
                del self.installed_paths[SrcDestMACPair(dst, src)]

    def migrate(self, mac):
        self.remove_host(mac)
        # Remove all the installed flows with the src MAC
        for k,v in self.installed_paths.items():
            if k.src == mac or k.dst == mac:
                self.uninstall_path(k.src, k.dst, v)

        # Find current hypervisor
        current_hypervisor = self.get_hypervisor(mac)

        # Update the topology
        self.topology.remove_edge(current_hypervisor, mac)
        # self.topology.add_edge(new_hypervisor, mac, type='link')
        # self.topology.add_edge(new_hypervisor, mac, type='link', obj=NetworkLink(NetworkPort(src_mac, None), NetworkPort(dpid_to_str(datapath.id), in_port)))


    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
            pkt = packet.Packet(array.array('B', ev.msg.data))

            # Ignore lldp packets
            if pkt.get_protocol(lldp.lldp):
                return

            # Parse the packets
            ethernetp = pkt.get_protocol(ethernet.ethernet)
            arpp = pkt.get_protocol(arp.arp)
            ipp = pkt.get_protocol(ipv4.ipv4)

            # Get datapath, protocol and protocol parser
            # print dir(ev.msg)
            datapath = ev.msg.datapath
            ofproto = datapath.ofproto
            parser = datapath.ofproto_parser

            # In port changes depending on the parser
            in_port = ev.msg.match['in_port']

            # We handle ARP packet
            if arpp:
                # print 'got arp packet', arpp, hex(datapath.id)

                # Got an host add it to the topology
                self.add_host(datapath, in_port, arpp.src_mac, arpp.src_ip)
                
                # If the packet is an ARP request
                if arpp.opcode == arp.ARP_REQUEST:
                    # Get the payload if no_buffer is specified fixes bug in OVS
                    data = None
                    if ev.msg.buffer_id == ofproto.OFP_NO_BUFFER:
                        data = ev.msg.data

                    # Send the packet out
                    out = parser.OFPPacketOut(
                        datapath=datapath,
                        buffer_id=ev.msg.buffer_id,
                        in_port=in_port,
                        actions=[parser.OFPActionOutput(ofproto.OFPP_FLOOD)],
                        data=data
                    )
                    datapath.send_msg(out)


                # If the packet is an ARP reply
                elif arpp.opcode == arp.ARP_REPLY:
                    # print 'arp response', arpp

                    if arpp.dst_mac == CONTROLLER_MAC:
                        # print 'controller generated mac request', datapath.id, in_port
                        return 

                    path = self.install_path(arpp.src_mac, arpp.dst_mac)
                    if path == None:
                        print 'no path between source and destination'
                        return

                    # Send the ARP_response
                    if dpid_to_str(datapath.id) in path:
                        idx = path.index(dpid_to_str(datapath.id))


                        dst_port = self.topology[dpid_to_str(datapath.id)][path[idx+1]]['obj'].getPortById(dpid_to_str(datapath.id)).port
                        data = None
                        if ev.msg.buffer_id == ofproto.OFP_NO_BUFFER:
                            data = ev.msg.data

                        out = parser.OFPPacketOut(
                            datapath=datapath,
                            buffer_id=ev.msg.buffer_id,
                            in_port=in_port,
                            actions=[parser.OFPActionOutput(dst_port)],
                            data=data
                        )
                        datapath.send_msg(out)
                    else:
                        print 'weird switch not on the path ... got packet from {} to {} at switch {} path is {}'.format(arpp.src_mac, arpp.dst_mac, dpid_to_str(datapath.id), path)


    def discover_host(self, ip):
        e = ethernet.ethernet(
            dst='ff:ff:ff:ff:ff:ff',
            src=CONTROLLER_MAC,
            ethertype=ether.ETH_TYPE_ARP
        )

        a = arp.arp(
            src_ip='0.0.0.0',
            src_mac=CONTROLLER_MAC,
            dst_ip=ip,
            dst_mac='ff:ff:ff:ff:ff:ff'
        )

        p = packet.Packet()
        p.add_protocol(e)
        p.add_protocol(a)
        p.serialize()

        switch = None
        for n,d in self.topology.nodes_iter(data=True):
            if d.get('type') == 'switch':
                switch = d.get('obj')

        if switch:
            print 'discovering', ip
            datapath = switch.dp
            ofproto = datapath.ofproto
            parser = datapath.ofproto_parser
            out = parser.OFPPacketOut(
                datapath=datapath,
                buffer_id=ofproto.OFP_NO_BUFFER,
                in_port=ofproto.OFPP_CONTROLLER,
                actions=[parser.OFPActionOutput(ofproto.OFPP_FLOOD)],
                data=p.data
            )
            datapath.send_msg(out)