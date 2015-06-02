#!/usr/bin/python

import httplib
import time
import json
import sys
from threading import Timer
import libvirt
import sys
import random
from operator import itemgetter, attrgetter, methodcaller
import numpy as np

def dpid_to_str(dpid):
    return '{:016x}'.format(dpid)

hypervisors_dpid_to_hostname = {
	'000090b11c876975': 'node-03.data',
	'000090b11c876c75': 'node-01.data',
	'000090b11c876ccd': 'node-02.data',
	'000090b11c876c55': 'node-06.data',
	'000090b11c876bae': 'node-04.data',
	'000090b11c876eac': 'node-08.data',
	'000090b11c876a82': 'node-07.data',
	'000090b11c876ead': 'node-05.data',
}

vm_mac_to_name = {
	'52:54:00:57:d7:64': 'vm1',
	'52:54:00:cb:91:86': 'vm2',
	'52:54:00:0c:82:68': 'vm3',
	'52:54:00:94:b3:1b': 'vm4',
	'52:54:00:58:e7:ee': 'vm5',
	'52:54:00:33:21:4b': 'vm6',
	'52:54:00:38:32:5b': 'vm7',
	'52:54:00:e1:38:21': 'vm8',
	'52:54:00:75:2e:ea': 'vm9',
	'52:54:00:13:03:a3': 'vm10',
	'52:54:00:23:58:29': 'vm11',
	'52:54:00:59:1f:63': 'vm12',
	'52:54:00:da:ed:12': 'vm13',
	'52:54:00:c2:5e:dc': 'vm14',
	'52:54:00:63:09:87': 'vm15',
	'52:54:00:ff:3d:48': 'vm16',
	'52:54:00:53:74:87': 'vm17',
	'52:54:00:3c:4e:d4': 'vm18',
	'52:54:00:f3:e0:4f': 'vm19',
	'52:54:00:3e:4d:5a': 'vm20',
	'52:54:00:16:29:a8': 'vm21',
	'52:54:00:a3:99:1e': 'vm22',
	'52:54:00:b5:94:62': 'vm23',
	'52:54:00:4d:5e:c0': 'vm24',
}

def getallocation():
   allocation = {}
   for i in range(1, 25):
      allocation["vm"+str(i)] = None

   for hv in hypervisors_dpid_to_hostname.values():
      conn = libvirt.openReadOnly("qemu+ssh://"+hv+"/system")
      if conn == None:
         print 'Failed to open connection to the hypervisor', hv
         continue;
      for id in conn.listDomainsID():
         vm = conn.lookupByID(id)
         allocation[vm.name()] = hv

   return allocation

def migrate(allocation, vmname, destination):
   source = allocation[vmname]
   conn = libvirt.open("qemu+ssh://"+source+"/system")
   conn2 = libvirt.open("qemu+ssh://"+destination+"/system")
   if conn == None or conn2 == None:
      print 'Failed to open connection to one of the hypervisors', hv
      return;
   vm = conn.lookupByName(vmname)
   # 1 - live migration
   # 8 - persistent
   # 64 - shared disk, full copy
   # bitwise OR is needed
   fuel = ['some coffee', 'a doughnut', 'more coffee', 'some sugar']
   print 'Migrating %s from %s to %s... (get %s)' % (vmname, source, destination,random.choice(fuel))
   vm.migrate(conn2, 73)


def response_as_json(conn):
	resp = conn.getresponse()
	body = resp.read()
	return json.loads(body)

def calculate_cost_matrix(traffic_matrix, vm_placement, hypervisor_weight_matrix):
	cost_matrix = {}
	overall_cost_matrix = {}

	# Calculate the cost matrix
	for source,peers in traffic_matrix.items():
		# print vm_placement
		if source not in vm_placement:
			# print 'skipping unknown {}'.format(source)
			continue

		for peer,traffic_rate in peers.items():
			if peer not in vm_placement:
				# print 'skipping unknown {}'.format(peer)
				continue

			# HACK
			if '90:b1:1c:87:72:c5' in [ source, peer ]:
				continue

			# Get the weight between source and pair
			source_hypervisor_dpid = vm_placement[source]
			peer_hypervisor_dpid = vm_placement[peer]

			if not (source_hypervisor_dpid and peer_hypervisor_dpid):
				# skipping the vm placement is unknown
				continue

			weight = hypervisor_weight_matrix[source_hypervisor_dpid][peer_hypervisor_dpid]['total_cost']
			# print "weight from {} to {} is {}".format(source_hypervisor_dpid, peer_hypervisor_dpid, weight)

			# print traffic_rate, weight
			cost_matrix.setdefault(source, {})[peer] = traffic_rate * weight
			# print cost_matrix

		# Calculate the overall cost matrix
		# print cost_matrix
		if source in cost_matrix:
			overall_cost_matrix[source] = sum(cost_matrix[source].values())
		# else:
			# print '{} not in the cost_matrix'.format(source)

	# Calculate the total cost
	total_cost = sum(overall_cost_matrix.values())

	return cost_matrix, overall_cost_matrix, total_cost

def cost_calculated(cost_matrix, overall_cost_matrix, total_cost):
	print '>> Current cost is {}'.format(total_cost)

	# Sort Matrix by communication cost
	sorted_overall_cost_matrix = sorted(overall_cost_matrix.iteritems(), key=lambda x: x[1])[::-1]

	# For each VM in the cost matrix (sorted by cost)
	for vm, communication_cost in sorted_overall_cost_matrix:
		# Keep a list of the potential hypervisor to migrate to and the cost of the network if migration occurs
		potential_hypervisors = []

		# Calculate the cost for if migrated to every potential hypervisor
		for hypervisor in hypervisor_dpid_to_mac.keys():
			if hypervisor == vm_placement[vm]:
				# Ignore the current hypervisor
				continue

			# Change the current place to the target hypervisor (hypothetical)
			potential_vm_placement = vm_placement.copy()
			potential_vm_placement[vm] = hypervisor

			potential_cost_matrix, potential_overall_cost_matrix, potential_total_cost = calculate_cost_matrix(traffic_matrix, potential_vm_placement, hypervisor_weight_matrix)
				
			potential_hypervisors.append((hypervisor, potential_total_cost))

		# Sorted list of hypervisors VM could be migrated to
		potential_hypervisors = sorted(potential_hypervisors, key=itemgetter(1))

		# Display
		for potential_hypervisor_dpid, potential_hypervisor_cost in potential_hypervisors:
			# print potential_hypervisor_dpid, potential_hypervisor_cost
			print 'Migrating VM {} to {} result in total cost of {}'.format(vm, potential_hypervisor_dpid, potential_hypervisor_cost)

			if total_cost > potential_hypervisor_cost:
				# Keep track of the selection, avoid migrating when system is not stable
				cost_calculated.rounds.append((total_cost, potential_total_cost, vm))
				cost_calculated.rounds = cost_calculated.rounds[-3:]

				if len(cost_calculated.rounds) != 3 or len([ x for x in cost_calculated.rounds if x[2] == vm ]) != 3:
					print 'Network unstable'
					return

				# Calculate the relative standard deviation of the costs
				rounds_costs = [ x[0] for x in cost_calculated.rounds ]
				rsd = np.std(rounds_costs) / np.average(rounds_costs) * 100

				if rsd < 5:
					vm_name = vm_mac_to_name[vm]
					target_hypervisor = hypervisors_dpid_to_hostname[potential_hypervisor_dpid]

					print '**** Migrating {} to {} ****'.format(vm_name, target_hypervisor)
					migrate(getallocation(), vm_name, target_hypervisor)

					conn.request("POST", "/v1.0/sdnmgmt/migrate?mac={}".format(vm))
					conn.getresponse().read()
				else:
					print 'Network unstable, relative standard deviation of {}'.format(rsd)

				return
cost_calculated.rounds = [] # [(total_cost, potential_total_cost, vmselected)]

log = open('logs.json', 'w')

### Controller endpoint
conn = httplib.HTTPConnection("localhost", 8080)

# First we query the hypervisors until all of them are placed/initialised
hypervisor_mac_to_dpid = {}
hypervisor_dpid_to_mac = {}
hypervisors_ip = ['10.0.0.1','10.0.0.2','10.0.0.3','10.0.0.4', '10.0.0.5','10.0.0.6','10.0.0.7','10.0.0.8']
while len(hypervisor_mac_to_dpid) < len(hypervisors_ip):
# Send hypervisor discovery
	conn.request("GET", "/v1.0/sdnmgmt/discovery?dst={}".format(','.join(hypervisors_ip)))
	conn.getresponse().read()

	conn.request("GET", "/v1.0/sdnmgmt/hypervisors")
	hypervisor_mac_to_dpid = response_as_json(conn)
	hypervisor_dpid_to_mac = { v: k for k, v in hypervisor_mac_to_dpid.items() }
		
	print '{}/{} hypervisors initialised, waiting ...'.format(len(hypervisor_mac_to_dpid), len(hypervisors_ip))
	time.sleep(2)

print '{} hypervisors initialised, starting orchestration'.format(len(hypervisor_mac_to_dpid))
#hack
hypervisor_dpid_to_mac.pop("0000000000000004", None)
hypervisor_mac_to_dpid.pop("90:b1:1c:87:72:c5", None)

# Create a hypervisor to hypervisors weight map
hypervisor_weight_matrix = {}
for dpid in hypervisor_dpid_to_mac.keys():
	conn.request("GET", "/v1.0/sdnmgmt/cost?src={}".format(dpid))
	h2h_costs = response_as_json(conn)

	for destination, cost in h2h_costs.items():
		hypervisor_weight_matrix.setdefault(dpid, {})[destination] = cost

log.write(json.dumps(hypervisor_weight_matrix))
log.write('\n')

# Now that all the hypervisors are initiliased, need to trigger a flow stat requests
last_migration_times = {}
selected_to_migrate = None
while True:
	conn.request("POST", "/v1.0/sdnmgmt/query")
	conn.getresponse().read()

	# Wait a bit for the switches to reply (hacky ...)
	time.sleep(1)

	# Query the current flow stats	
	conn.request("GET", "/v1.0/sdnmgmt/view")
	stats = response_as_json(conn)

	### Get the VM's hypervisors
	conn.request("GET", "/v1.0/sdnmgmt/placement")
	vm_placement = response_as_json(conn)

	### Keep a matrix of the traffic rate
	traffic_matrix = {}
	for k, v in stats.items():
		# For the source and destination of the traffic
		mac1 = v['endpoints']['mac1']
		mac2 = v['endpoints']['mac2']

		traffic_rate = v['traffic_rate']
		if traffic_rate > 0:
			traffic_matrix.setdefault(mac1, {})[mac2] = traffic_rate
			traffic_matrix.setdefault(mac2, {})[mac1] = traffic_rate
	# print traffic_matrix

	### Compute the weight matrix
	weight_matrix = {}
	for k, v in stats.items():
		# Cost can be blank if the path between source and dest in unknown
		if not v.get('cost'):
			continue

		# For the source and destination of the traffic
		mac1 = v['endpoints']['mac1']
		mac2 = v['endpoints']['mac2']

		cost = v['cost']['total_cost']
		if cost > 0:
			weight_matrix.setdefault(mac1, {})[mac2] = cost

	cost_matrix, overall_cost_matrix, total_cost = calculate_cost_matrix(traffic_matrix, vm_placement, hypervisor_weight_matrix)

	log.write(json.dumps(vm_placement))
	log.write(json.dumps(cost_matrix))
	log.write(json.dumps(overall_cost_matrix))
	log.write('\n')



	# print 'Total cost {}'.format(total_cost)
	
	cost_calculated(cost_matrix, overall_cost_matrix, total_cost)
	time.sleep(4)


	# # print cost_matrix
	# sorted_overall_cost_matrix = sorted(overall_cost_matrix.iteritems(), key=lambda x: x[1])[::-1]

	# for vm, communication_cost in sorted_overall_cost_matrix:		
	# 	# Check if this machine can be migrated
	# 	# last_migration_time = last_migration_times.get(vm)
	# 	# if last_migration_time and time.time() - last_migration_time < 5*60:
	# 		# print 'VM {} has been migrated recently, skipping migration'.format(vm)
	# 		# continue

	# 	# Check if the system is stable
	# 	# if selected_to_migrate and selected_to_migrate[0] == vm and selected_to_migrate[1] > 1 and abs((selected_to_migrate[1] - communication_cost) / selected_to_migrate[1]) < 0.05:
	# 	print 'System is stable, migrating {}'.format(vm)
	# 	potential_hypervisors = []

	# 	# Calculate the cost for every potential hypervisor
	# 	for hypervisor in hypervisor_dpid_to_mac.keys():
	# 		if hypervisor == vm_placement[vm]:
	# 			continue

	# 		# Find the destination hypervisor that will reduce the total cost the most
	# 		potential_vm_placement = vm_placement.copy()
	# 		potential_vm_placement[vm] = hypervisor

	# 		potential_cost_matrix, potential_overall_cost_matrix, potential_total_cost = calculate_cost_matrix(traffic_matrix, potential_vm_placement, hypervisor_weight_matrix)
				
	# 		potential_hypervisors.append((hypervisor, potential_total_cost))

	# 	potential_hypervisors = sorted(potential_hypervisors, key=lambda x: x[1])
	# 	# best_cost = potential_hypervisors[0][1]
	# 		# if best_cost < total_cost:
	# 	for potential_hypervisor_dpid, potential_hypervisor_cost in potential_hypervisors:
	# 		print potential_hypervisor_dpid, potential_hypervisor_cost
	# 		print 



	# 				hypervisor_name = hypervisors_by_mac[potential_hypervisor_mac]['name']
	# 				hypervisor_id = hypervisors_by_name[hypervisor_name].id
	# 				h = nt.hypervisors.get(hypervisor_id)

	# 				# Migrate if the number of vcpus used is lower than 8
	# 				if h.vcpus_used < 7:
	# 					print "migrating {} to {}".format(vm, hypervisor_name)
	# 					print 'migrating {} to reduce cost from {} to {}'.format(vm, total_cost, best_cost)

	# 					## Now try to find the vm by ip
	# 					sourceServer = None
	# 					for s in nt.servers.list():
	# 						for addresses in s.addresses['vmnet']:
	# 							if addresses['OS-EXT-IPS-MAC:mac_addr'] == vm:
	# 								sourceServer = s

	# 					if sourceServer:
	# 						print "Starting live migration ..."
	# 						last_migration_times[vm] = time.time()
	# 						sourceServer.live_migrate(host=hypervisor_name)
	# 						time.sleep(3)
	# 						conn.request("POST", "/v1.0/sdnmgmt/migrate?mac={}&hypervisor={}".format(vm, potential_hypervisor_mac))
	# 						conn.getresponse().read()
	# 						break
	# 					else:
	# 						print "unable to find the server in the hypervisor manager"
	# 				else:
	# 					print 'cannot migrate to {} vcpus limit reached'.format(hypervisor_name)
	# 		else:
	# 			print 'No cost benefit in migrating {}'.format(vm)
	# 	else:
	# 		print 'system is not stable', selected_to_migrate


	# 	selected_to_migrate = (vm, communication_cost)
	# 	break

				# for source, peers in weight_matrix.items():
					# for peer,weight in peers.items():
						# potential_weight_matrix[source][peer] = cost(source.hypervisor -> peer.hypervisors)


			# Want to recalculate the cost_matrix with the weight matrix

			### Find the VM with the pairwise VM with the highest communication cost
			# dst, dst_cost = sorted(cost_matrix[vm].iteritems(), key=lambda x: x[1])[-1]
			# print 'Pairwise VM destination {} {}'.format(dst, dst_cost)

			### Need to find a hypervisor that can host this VM close to the dst
			# conn.request("GET", "/v1.0/sdnmgmt/cost?src={}".format(dst))
			# h2h_costs = response_as_json(conn)


	### Compute overall cost matrix
	# overall_cost_matrix = { k: sum(v.values()) for k,v in cost_matrix.items() }
	# Sorted descending, first item is the highest cost
	# sorted_overall_cost_matrix = sorted(overall_cost_matrix.iteritems(), key=lambda x: x[1])[::-1]

	### Compute the total cost
	# total_cost = sum(overall_cost_matrix.values())

	###
	# for vm, communication_cost in sorted_overall_cost_matrix:
		# Calculate the cost of the system if the migration occured
		# for peer, peer_cost in cost_matrix[vm].items():
			# print peer, peer_cost


	# 	# Check if this machine can be migrated
	# 	last_migration_time = last_migration_times.get(vm)
	# 	if last_migration_time and time.time() - last_migration_time < 5*60:
	# 		print 'VM {} has been migrated recently, skipping migration'.format(vm)
	# 		continue

	# 	# Check if the system is stable
	# 	if selected_to_migrate and selected_to_migrate[0] == vm and abs((selected_to_migrate[1] - communication_cost) / selected_to_migrate[1]) < 0.05:
	# 		print 'System is stable, migrating {}'.format(vm)

	# 		### Find the VM with the pairwise VM with the highest communication cost
	# 		dst, dst_cost = sorted(cost_matrix[vm].iteritems(), key=lambda x: x[1])[-1]
	# 		print 'Pairwise VM destination {} {}'.format(dst, dst_cost)

	# 		### Need to find a hypervisor that can host this VM close to the dst
	# 		conn.request("GET", "/v1.0/sdnmgmt/cost?src={}".format(dst))
	# 		h2h_costs = response_as_json(conn)

	# 		# Iterate over the hypervisors with the lowest communication cost
	# 		for i in sorted(h2h_costs.iteritems(), key=lambda x: x[1]['total_cost']):
	# 			hypervisor_mac = i[0]
	# 			hypervisor_name = hypervisors_by_mac[hypervisor_mac]['name']
	# 			hypervisor_id = hypervisors_by_name[hypervisor_name].id
	# 			h = nt.hypervisors.get(hypervisor_id)

	# 			# Migrate if the number of vcpus used is lower than 8
	# 			if h.vcpus_used < 8:
	# 				print "migrating {} to {}".format(vm, hypervisor_name)

	# 				## Now try to find the vm by ip
	# 				sourceServer = None
	# 				for s in nt.servers.list():
	# 					for addresses in s.addresses['vmnet']:
	# 						if addresses['OS-EXT-IPS-MAC:mac_addr'] == vm:
	# 							sourceServer = s

	# 				if sourceServer:
	# 					print "Starting live migration ..."
	# 					last_migration_times[vm] = time.time()
	# 					sourceServer.live_migrate(host=hypervisor_name)
	# 					time.sleep(3)
	# 					conn.request("POST", "/v1.0/sdnmgmt/migrate?mac={}&hypervisor={}".format(vm, hypervisor_mac))
	# 					conn.getresponse().read()
	# 					break
	# 				else:
	# 					print "unable to find the server in the hypervisor manager"



	# 	selected_to_migrate = (vm, communication_cost)
	# 	print 'Selected to migrate {} {}, Total cost {}'.format(selected_to_migrate[0], selected_to_migrate[1], total_cost)
	# 	break

	# time.sleep(4)


	# highest_cost = None
	# highest_val = None
	# for k, v in cost_matrix.items():
	# 	cost = sum(v.values())
	# 	overall_cost_matrix[k] = cost

	# 	if not highest_cost or overall_cost_matrix[highest_cost] < cost:
	# 		highest_cost = k
	# 		highest_val = overall_cost_matrix[highest_cost]


	# print 'Host with highest communication cost is', highest_cost, highest_val
	# print cost_matrix.get(highest_cost)
	# print 'Total cost is', total_cost

	# ### Check if the system is stable, last measurement was the same VM with 5% traffic difference
	# if last_highest_cost and last_highest_cost == highest_cost:
	# 	percent_diff = abs((last_highest_val - highest_val) / last_highest_val)
	# 	print percent_diff

	# 	if percent_diff < 0.05:
	# 		### Find the VM with the pairwise VM with the highest communication cost
	# 		dst, dst_cost = sorted(cost_matrix[highest_cost].iteritems(), key=lambda x: x[1])[-1]

	# 		### Need to find a hypervisor that can host this VM close to the dst
	# 		if highest_cost:
	# 			conn.request("GET", "/v1.0/sdnmgmt/cost?src={}".format(dst))
	# 			h2h_costs = response_as_json(conn)

	# 			# Iterate over the hypervisors with the lowest communication cost
	# 			for i in sorted(h2h_costs.iteritems(), key=lambda x: x[1]['total_cost']):
	# 				hypervisor_mac = i[0]
	# 				hypervisor_name = hypervisors_by_mac[hypervisor_mac]['name']
	# 				hypervisor_id = hypervisors_by_name[hypervisor_name].id
	# 				h = nt.hypervisors.get(hypervisor_id)

	# 				# Migrate if the number of vcpus used is lower than 8
	# 				if h.vcpus_used < 8:
	# 					print "migrating {} to {}".format(highest_cost, hypervisor_name)

	# 					## Now try to find the vm by ip
	# 					sourceServer = None
	# 					for s in nt.servers.list():
	# 						for addresses in s.addresses['vmnet']:
	# 							if addresses['OS-EXT-IPS-MAC:mac_addr'] == highest_cost:
	# 								sourceServer = s

	# 					if sourceServer:
	# 						print "Starting live migration, sleeping for 20s"
	# 						sourceServer.live_migrate(host=hypervisor_name)
	# 						time.sleep(3)
	# 						conn.request("POST", "/v1.0/sdnmgmt/migrate?mac={}&hypervisor={}".format(highest_cost, hypervisor_mac))
	# 						conn.getresponse().read()
	# 						time.sleep(17)

	# 						# Clear the values so next round has to request new values
	# 						highest_val = None
	# 						highest_cost = None
	# 					else:
	# 						print "unable to find the server in the hypervisor manager"

	# 					break

	# last_highest_val = highest_val
	# last_highest_cost = highest_cost

	# #
	# time.sleep(4)