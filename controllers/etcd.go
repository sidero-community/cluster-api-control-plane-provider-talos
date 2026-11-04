// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// etcdHealth summarizes what etcdHealthcheck observed, so that callers can tell
// a degraded etcd cluster that still has quorum from one that lost it.
type etcdHealth struct {
	// machines is the number of machines that are expected to run an etcd member.
	machines int
	// inspected is the number of machines whose etcd service status could be retrieved.
	inspected int
	// healthy is the number of machines running a healthy etcd service that is a voting member.
	healthy int
	// members is the number of voting members in the etcd member list, or 0 if no member list could be retrieved.
	// Learners don't vote, so they don't count towards quorum.
	members int
	// excluded is the number of voting members that belong to machines excluded from the check (being deleted or
	// leaving etcd): they are not inspected, and may still be running.
	excluded int
	// statuses is what was observed on each machine that is expected to run an etcd member, by machine name. Machines
	// that are being deleted or leaving etcd are not inspected, so they have no entry; a machine without an entry
	// must be treated as unhealthy.
	statuses map[string]etcdMachineStatus
}

// etcdMachineStatus is what the etcd health check observed on a control plane machine.
type etcdMachineStatus struct {
	// healthy reports whether the etcd service of the machine is running and healthy; it is false if the machine
	// could not be inspected.
	healthy bool
	// members is the etcd member list as seen by the machine, or nil if it could not be listed: it is only listed
	// when the etcd service is healthy.
	members []*machineapi.EtcdMember
	// listErr is why the etcd member list could not be listed, if it could not.
	listErr error
}

// etcdMachineProbe is what the Talos API of a control plane machine returned about etcd.
type etcdMachineProbe struct {
	// inspectErr is set if the etcd service status could not be retrieved.
	inspectErr error
	services   []talosclient.ServiceInfo
	// memberList is only retrieved when the etcd service is healthy.
	memberList *machineapi.EtcdMemberListResponse
	listErr    error
}

// probeEtcd retrieves the etcd service status of the machine and, if it is healthy, the etcd member list as seen by
// the machine, within its own timeout.
func (r *TalosControlPlaneReconciler) probeEtcd(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine clusterv1.Machine) etcdMachineProbe {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	// the client created has endpoints which point to a single machine
	c, err := r.etcdClientFor(ctx, tcp, machine)
	if err != nil {
		return etcdMachineProbe{inspectErr: err}
	}

	defer c.Close() //nolint:errcheck

	svcs, err := c.ServiceInfo(ctx, etcdService)
	if err != nil {
		return etcdMachineProbe{inspectErr: err}
	}

	probe := etcdMachineProbe{services: svcs}

	if etcdServiceError(svcs, machine.Name) == nil {
		probe.memberList, probe.listErr = c.EtcdMemberList(ctx, &machineapi.EtcdMemberListRequest{})
	}

	return probe
}

// quorum returns the number of healthy voting members etcd needs to keep quorum.
func (h etcdHealth) quorum() int {
	total := h.members
	if total == 0 {
		total = h.machines
	}

	return total/2 + 1
}

// hasQuorum reports whether enough etcd members are healthy for etcd to keep quorum.
func (h etcdHealth) hasQuorum() bool {
	return h.healthy >= h.quorum()
}

// mayHaveQuorum reports whether etcd may still have quorum, assuming that the machines that could not be
// inspected, and the members of the machines excluded from the check, run a healthy etcd member. When it is false,
// etcd lost quorum whatever the state of those machines.
func (h etcdHealth) mayHaveQuorum() bool {
	return h.healthy+(h.machines-h.inspected)+h.excluded >= h.quorum()
}

// conditionReason returns the reason of the EtcdClusterHealthy condition when the healthcheck failed.
func (h etcdHealth) conditionReason() string {
	switch {
	case h.inspected == 0:
		return controlplanev1.EtcdClusterInspectionFailedReason
	case h.hasQuorum():
		return controlplanev1.EtcdClusterUnhealthyReason
	case h.mayHaveQuorum():
		// Whether etcd has quorum depends on machines that could not be inspected.
		return controlplanev1.EtcdClusterInspectionFailedReason
	default:
		return controlplanev1.EtcdClusterQuorumLostReason
	}
}

// etcdHealthcheck checks etcd on every control plane machine that is not being deleted or leaving etcd.
//
// It returns the first problem found as an error, but always inspects all machines so that the returned
// etcdHealth reflects the whole cluster. The machines are inspected concurrently, each with its own timeout, so that
// unreachable machines neither prevent the others from being inspected nor add up their timeouts.
func (r *TalosControlPlaneReconciler) etcdHealthcheck(ctx context.Context, tcp *controlplanev1.TalosControlPlane, ownedMachines []clusterv1.Machine) (etcdHealth, error) {
	machines := healthCheckable(ownedMachines)

	// If every owned machine is on its way out there is nothing left to verify. Reporting etcd healthy here would
	// open the scale-down/rollout gate without a single member checked, so treat it as unhealthy.
	if len(ownedMachines) > 0 && len(machines) == 0 {
		return etcdHealth{}, fmt.Errorf("all %d owned control plane machines are excluded from the etcd health check", len(ownedMachines))
	}

	membership := newEtcdMembership(ownedMachines, machines)

	excludedMachines := make([]clusterv1.Machine, 0, len(ownedMachines)-len(machines))

	for _, machine := range ownedMachines {
		if !slices.ContainsFunc(machines, func(m clusterv1.Machine) bool { return m.Name == machine.Name }) {
			excludedMachines = append(excludedMachines, machine)
		}
	}

	excludedNames := etcdMemberNames(excludedMachines)

	params := make([]any, 0, len(machines)*2)
	for _, machine := range machines {
		params = append(params, "node", machine.Name)
	}

	r.Log.Info("verifying etcd health on all nodes", params...)

	probes := make([]etcdMachineProbe, len(machines))

	var wg sync.WaitGroup

	for i, machine := range machines {
		wg.Go(func() {
			probes[i] = r.probeEtcd(ctx, tcp, machine)
		})
	}

	wg.Wait()

	health := etcdHealth{
		machines: len(machines),
		statuses: make(map[string]etcdMachineStatus, len(machines)),
	}

	// the member list reported by the previous node, to check that every node sees the same cluster
	var (
		previousNode    string
		previousMembers map[string]struct{}
	)

	var firstErr error

	// The probes are evaluated in the order of the machines, so that the result doesn't depend on which probe
	// finished first.
	for i, machine := range machines {
		probe := probes[i]

		health.statuses[machine.Name] = etcdMachineStatus{}

		if err := func() error {
			if probe.inspectErr != nil {
				return probe.inspectErr
			}

			health.inspected++

			// check that etcd service is healthy on the node
			if err := etcdServiceError(probe.services, machine.Name); err != nil {
				return err
			}

			health.healthy++

			status := etcdMachineStatus{healthy: true, listErr: probe.listErr}

			if probe.listErr == nil && len(probe.memberList.GetMessages()) > 0 {
				// The client points to a single machine, so only 1 message should return.
				status.members = probe.memberList.Messages[0].Members
			}

			health.statuses[machine.Name] = status

			if probe.listErr != nil {
				return probe.listErr
			}

			for _, message := range probe.memberList.Messages {
				votingMembers, excludedMembers := 0, 0

				for _, member := range message.Members {
					if member.IsLearner {
						continue
					}

					votingMembers++

					if _, ok := excludedNames[strings.ToLower(member.Hostname)]; ok {
						excludedMembers++
					}
				}

				health.members = max(health.members, votingMembers)
				health.excluded = max(health.excluded, excludedMembers)

				// a learner doesn't vote, so its health doesn't help etcd keep quorum
				if member := findEtcdMember(message.Members, &machine); member != nil && member.IsLearner {
					health.healthy--
				}

				node := message.Metadata.GetHostname()

				members, err := membership.verify(node, message.Members)
				if err != nil {
					return err
				}

				if previousMembers != nil {
					if err := compareEtcdMembers(node, members, previousNode, previousMembers); err != nil {
						return err
					}
				}

				previousNode, previousMembers = node, members
			}

			return nil
		}(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("error checking etcd health on machine %q: %w", machine.Name, err)
		}
	}

	return health, firstErr
}

// etcdService is the Talos service running the etcd member of a control plane machine.
const etcdService = "etcd"

// etcdServiceError returns an error unless the etcd service is running and healthy on every node of the response. A
// response without the service, e.g. in maintenance mode, is an error too. Errors are prefixed with the node name
// the response carries, or with machineName when it carries none.
func etcdServiceError(svcs []talosclient.ServiceInfo, machineName string) error {
	if len(svcs) == 0 {
		return fmt.Errorf("%s: service %q not found", machineName, etcdService)
	}

	for _, svc := range svcs {
		node := svc.Metadata.GetHostname()
		if node == "" {
			node = machineName
		}

		if len(svc.Service.GetEvents().GetEvents()) == 0 {
			return fmt.Errorf("%s: no events recorded yet for service %q", node, etcdService)
		}

		lastEvent := svc.Service.Events.Events[len(svc.Service.Events.Events)-1]
		if lastEvent.State != "Running" {
			return fmt.Errorf("%s: service %q not in expected state %q: current state [%s] %s", node, etcdService, "Running", lastEvent.State, lastEvent.Msg)
		}

		if !svc.Service.GetHealth().GetHealthy() {
			return fmt.Errorf("%s: service is not healthy: %s", node, etcdService)
		}
	}

	return nil
}

// nodeNamesForMachine returns the host names that can identify a machine's etcd member: the nodeRef name and every
// MachineHostName address (some infrastructure providers report the infrastructure machine name there, which may not
// match the node hostname). Each name is lowercased and has any domain suffix trimmed (nodeRef names can be FQDNs,
// e.g. on AWS); empty names are skipped.
func nodeNamesForMachine(machine *clusterv1.Machine) []string {
	candidates := []string{machine.Status.NodeRef.Name}

	for _, address := range machine.Status.Addresses {
		if address.Type == clusterv1.MachineHostName {
			candidates = append(candidates, address.Address)
		}
	}

	names := make([]string, 0, len(candidates))

	for _, hostname := range candidates {
		hostname, _, _ = strings.Cut(hostname, ".")
		if hostname == "" {
			continue
		}

		names = append(names, strings.ToLower(hostname))
	}

	return names
}

// etcdMemberNames returns the set of host names (see nodeNamesForMachine) an etcd member may carry to belong to one
// of the machines. Machines without a nodeRef have no known names and contribute nothing.
func etcdMemberNames(machines []clusterv1.Machine) map[string]struct{} {
	names := map[string]struct{}{}

	for i := range machines {
		for _, name := range nodeNamesForMachine(&machines[i]) {
			names[name] = struct{}{}
		}
	}

	return names
}

// etcdMembership holds what an etcd member list is verified against: the host names of the machines that must be
// etcd members (expected, keyed by machine name), the machines that must be members but whose names are not known
// yet because they have no nodeRef (pending), and the host names of every owned machine (owned), which decide
// whether a member is an orphan.
type etcdMembership struct {
	expected map[string][]string
	pending  []string
	owned    map[string]struct{}
}

// newEtcdMembership builds the membership for the owned machines, of which machines is the subset that takes part in
// the etcd health check (see healthCheckable).
func newEtcdMembership(ownedMachines, machines []clusterv1.Machine) etcdMembership {
	membership := etcdMembership{
		expected: make(map[string][]string, len(machines)),
		owned:    etcdMemberNames(ownedMachines),
	}

	for i := range machines {
		names := nodeNamesForMachine(&machines[i])
		if len(names) == 0 {
			membership.pending = append(membership.pending, machines[i].Name)

			continue
		}

		membership.expected[machines[i].Name] = names
	}

	return membership
}

// verify checks the etcd member list reported by node and returns the set of member host names, lowercased, for
// comparison across nodes.
//
// Every machine that must be a member has to have one, matched by any of its host names. A machine that must be a
// member but has no nodeRef yet (it is new and the kubelet has not registered) cannot be matched by name, so it is
// accounted for by count: each such machine claims one member matching no owned machine. Any further unmatched member
// is an orphan (auditEtcd force-removes these), and fewer unmatched members than pending machines means a new machine
// has not joined etcd yet. A member of an excluded machine still matches an owned machine, so it is tolerated; that
// machine's member may also already be gone, which is expected, so it is not required.
func (m etcdMembership) verify(node string, members []*machineapi.EtcdMember) (map[string]struct{}, error) {
	present := make(map[string]struct{}, len(members))

	var unmatched []string

	for _, member := range members {
		name := strings.ToLower(member.Hostname)
		present[name] = struct{}{}

		if _, ok := m.owned[name]; !ok {
			unmatched = append(unmatched, member.Hostname)
		}
	}

	for machineName, names := range m.expected {
		if !slices.ContainsFunc(names, func(name string) bool {
			_, ok := present[name]

			return ok
		}) {
			return nil, fmt.Errorf("%s: etcd is missing a member for control plane machine %q", node, machineName)
		}
	}

	switch {
	case len(unmatched) > len(m.pending):
		return nil, fmt.Errorf("%s: etcd member %q does not match any control plane machine", node, unmatched[len(m.pending)])
	case len(unmatched) < len(m.pending):
		return nil, fmt.Errorf("%s: etcd is missing a member for %d of the control plane machines without a noderef yet %q",
			node, len(m.pending)-len(unmatched), m.pending)
	}

	return present, nil
}

// compareEtcdMembers reports a member list that differs from the one another node reported: each etcd member must see
// the same cluster, so a member known to only one of the two nodes is an error.
func compareEtcdMembers(node string, members map[string]struct{}, previousNode string, previous map[string]struct{}) error {
	for name := range members {
		if _, ok := previous[name]; !ok {
			return fmt.Errorf("%s: etcd member %q is not known to %s", node, name, previousNode)
		}
	}

	for name := range previous {
		if _, ok := members[name]; !ok {
			return fmt.Errorf("%s: etcd member %q known to %s is missing", node, name, previousNode)
		}
	}

	return nil
}

// etcdMemberMatchesMachine reports whether the etcd member runs on the machine, matched by any of its host names (see
// nodeNamesForMachine).
func etcdMemberMatchesMachine(member *machineapi.EtcdMember, machine *clusterv1.Machine) bool {
	return slices.Contains(nodeNamesForMachine(machine), strings.ToLower(member.Hostname))
}

// findEtcdMember returns the etcd member running on the machine, or nil if there is none.
func findEtcdMember(members []*machineapi.EtcdMember, machine *clusterv1.Machine) *machineapi.EtcdMember {
	for _, member := range members {
		if etcdMemberMatchesMachine(member, machine) {
			return member
		}
	}

	return nil
}

// identifyEtcdMember returns the etcd member running on the machine, or nil if there is none; machines are all the
// control plane machines. A machine without host names yet (see nodeNamesForMachine) can't be matched by name, so its
// member is found by elimination: it is the only member that matches no other machine, if every other machine has
// host names. known is false when it can't be told whether the machine has a member.
func identifyEtcdMember(members []*machineapi.EtcdMember, machine *clusterv1.Machine, machines []*clusterv1.Machine) (member *machineapi.EtcdMember, known bool) {
	if len(nodeNamesForMachine(machine)) > 0 {
		return findEtcdMember(members, machine), true
	}

	others := make([]*clusterv1.Machine, 0, len(machines))

	for _, other := range machines {
		if other.Name != machine.Name {
			others = append(others, other)
		}
	}

	var unmatched []*machineapi.EtcdMember

	for _, member := range members {
		if !slices.ContainsFunc(others, func(other *clusterv1.Machine) bool { return etcdMemberMatchesMachine(member, other) }) {
			unmatched = append(unmatched, member)
		}
	}

	switch {
	case len(unmatched) == 0:
		return nil, true
	case len(unmatched) == 1 && !slices.ContainsFunc(others, func(other *clusterv1.Machine) bool { return len(nodeNamesForMachine(other)) == 0 }):
		return unmatched[0], true
	default:
		return nil, false
	}
}

// forceEtcdLeave removes a given machine from the etcd cluster by telling another CP node to remove the member.
// This is used in times when the machine was deleted out from under us.
func (r *TalosControlPlaneReconciler) forceEtcdLeave(ctx context.Context, c etcdCalls, member *machineapi.EtcdMember) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	defer cancel()

	r.Log.Info("removing etcd member", "memberName", member.Hostname, "memberId", member.Id)

	return c.EtcdRemoveMemberByID(
		ctx,
		&machineapi.EtcdRemoveMemberByIDRequest{
			MemberId: member.Id,
		},
	)
}

// auditEtcd rolls through all etcd members to see if there's a matching controlplane machine
// It uses the first controlplane node returned as the etcd endpoint
func (r *TalosControlPlaneReconciler) auditEtcd(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster client.ObjectKey) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	defer cancel()

	machines, err := r.getControlPlaneMachinesForCluster(ctx, cluster)
	if err != nil {
		return err
	}

	if len(machines.Items) == 0 {
		return nil
	}

	for _, machine := range machines.Items {
		// nb: we'll assume any machine that doesn't have a noderef is new and we can audit later because
		//     otherwise a new etcd member can get removed before even getting the noderef set by the CAPI controllers.
		if !machine.Status.NodeRef.IsDefined() {
			return fmt.Errorf("some CP machines do not have a noderef")
		}
	}
	// Select the first CP machine that's not being deleted and has a noderef
	var designatedCPMachine clusterv1.Machine

	for _, machine := range machines.Items {
		if !machine.ObjectMeta.DeletionTimestamp.IsZero() || !machine.Status.NodeRef.IsDefined() {
			continue
		}

		designatedCPMachine = machine

		break
	}

	if designatedCPMachine.Name == "" {
		return fmt.Errorf("no CP machine which is not being deleted and has node ref")
	}

	c, err := r.etcdClientFor(ctx, tcp, designatedCPMachine)
	if err != nil {
		return err
	}

	defer c.Close() //nolint:errcheck

	response, err := c.EtcdMemberList(ctx, &machineapi.EtcdMemberListRequest{})
	if err != nil {
		return fmt.Errorf("error getting etcd members via %q: %w", designatedCPMachine.Name, err)
	}

	if len(response.Messages) == 0 {
		return fmt.Errorf("empty etcd member list response from machine %q", designatedCPMachine.Name)
	}

	// Only querying one CP node, so only 1 message should return.
	memberList := response.Messages[0]

	owned := etcdMemberNames(machines.Items)

	// For each etcd member, check that it matches one of the host names of the machines
	for _, member := range memberList.Members {
		if member.Hostname == "" {
			return fmt.Errorf("discovered etcd member with empty hostname: %s", member)
		}

		if _, present := owned[strings.ToLower(member.Hostname)]; !present {
			r.Log.Info("found etcd member that doesn't exist as controlplane machine", "member", member)

			if err = r.forceEtcdLeave(ctx, c, member); err != nil {
				return fmt.Errorf("error leaving etcd for member %q via machine %q", member, designatedCPMachine.Name)
			}
		}
	}

	return nil
}
