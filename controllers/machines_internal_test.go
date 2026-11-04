// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"time"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// machineOption mutates a test machine.
type machineOption func(*clusterv1.Machine)

// newMachine returns a bare machine with the given name, mutated by mutate if set.
func newMachine(name string, mutate func(*clusterv1.Machine)) clusterv1.Machine {
	m := clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if mutate != nil {
		mutate(&m)
	}

	return m
}

// markDeleting gives the machine a deletion timestamp.
func markDeleting(m *clusterv1.Machine) {
	now := metav1.Now()
	m.DeletionTimestamp = &now
}

// markLeaving marks the machine as leaving etcd.
func markLeaving(m *clusterv1.Machine) {
	m.Annotations = map[string]string{etcdLeavingAnnotation: "true"}
}

// etcdMembersFor returns an etcd member per host name, with ids starting at 1.
func etcdMembersFor(names ...string) []*machineapi.EtcdMember {
	members := make([]*machineapi.EtcdMember, 0, len(names))

	for i, name := range names {
		members = append(members, &machineapi.EtcdMember{Id: uint64(i + 1), Hostname: name})
	}

	return members
}

// newTestMachine returns a machine whose node carries the machine's name.
func newTestMachine(name string, created time.Time, opts ...machineOption) *clusterv1.Machine {
	m := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(created)},
		Status:     clusterv1.MachineStatus{NodeRef: clusterv1.MachineNodeReference{Name: name}},
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// withoutNodeRef strips the machine of its node, as a machine whose kubelet has not registered yet.
func withoutNodeRef() machineOption {
	return func(m *clusterv1.Machine) {
		m.Status.NodeRef = clusterv1.MachineNodeReference{}
	}
}
