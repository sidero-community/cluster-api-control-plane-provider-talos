// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// bootstrapFixture drives reconcileBootstrap against fakes for the two things it does to a
// node: ask whether it holds etcd data, and send it the bootstrap request.
type bootstrapFixture struct {
	*preTerminateFixture

	dataOn  map[string]bool
	dataErr map[string]error
	asked   []string

	bootstrapErr error
	bootstrapped []string
}

func newBootstrapFixture(t *testing.T, tcp *controlplanev1.TalosControlPlane, machines ...*clusterv1.Machine) *bootstrapFixture {
	t.Helper()

	bf := &bootstrapFixture{
		preTerminateFixture: newPreTerminateFixtureWithTCP(t, tcp, machines...),
		dataOn:              map[string]bool{},
		dataErr:             map[string]error{},
	}

	bf.r.etcdDataChecker = func(_ context.Context, _ *controlplanev1.TalosControlPlane, m clusterv1.Machine) (bool, error) {
		bf.asked = append(bf.asked, m.Name)

		if err := bf.dataErr[m.Name]; err != nil {
			return false, err
		}

		return bf.dataOn[m.Name], nil
	}

	bf.r.bootstrapper = func(_ context.Context, _ *controlplanev1.TalosControlPlane, m clusterv1.Machine) error {
		bf.bootstrapped = append(bf.bootstrapped, m.Name)

		return bf.bootstrapErr
	}

	return bf
}

func (bf *bootstrapFixture) run() (ctrl.Result, error) {
	return bf.r.reconcileBootstrap(context.Background(), bf.tcp, bf.machines.Items)
}

// bsNoAddress makes a machine one that reported no address yet.
func bsNoAddress(m *clusterv1.Machine) {
	m.Status.Addresses = nil
	m.Status.NodeRef = clusterv1.MachineNodeReference{}
}

// bsBootstrapped records machine as the bootstrapped one on the TalosControlPlane.
func bsBootstrapped(tcp *controlplanev1.TalosControlPlane, machine string) {
	tcp.Status.Bootstrapped = true
	tcp.Status.BootstrappedMachine = machine
}

func (bf *bootstrapFixture) expectHeld(t *testing.T, result ctrl.Result, reason, message string) {
	t.Helper()

	assert.Equal(t, bootstrapRetryAfter, result.RequeueAfter)
	assert.Empty(t, bf.bootstrapped, "nothing is bootstrapped while the bootstrap is held")

	c := conditions.Get(bf.tcp, string(controlplanev1.MachinesBootstrapped))
	require.NotNil(t, c)
	assert.Equal(t, reason, c.Reason)
	assert.Contains(t, c.Message, message)
}

func (bf *bootstrapFixture) expectBootstrapped(t *testing.T, machine string) {
	t.Helper()

	assert.True(t, bf.tcp.Status.Bootstrapped)
	assert.Equal(t, machine, bf.tcp.Status.BootstrappedMachine)

	c := conditions.Get(bf.tcp, string(controlplanev1.MachinesBootstrapped))
	require.NotNil(t, c)
	assert.Equal(t, controlplanev1.MachinesBootstrappedReason, c.Reason)
	assert.Contains(t, c.Message, machine)
}

func TestReconcileBootstrap_WaitsForTheFirstMachineToReportAnAddress(t *testing.T) {
	tcp := newPreTerminateTCP()
	bf := newBootstrapFixture(t, tcp, newRMMachine(tcp, "m1", time.Now(), bsNoAddress))

	result, err := bf.run()
	require.NoError(t, err)

	bf.expectHeld(t, result, controlplanev1.WaitingForTalosBootReason, "report an address")
	assert.False(t, bf.tcp.Status.Bootstrapped)
	assert.Empty(t, bf.asked, "a machine without an address cannot be asked anything")
}

// The oldest reachable machine is the one bootstrapped, as KCP's first machine is the init
// machine; a machine that cannot be reached yet does not hold it up.
func TestReconcileBootstrap_BootstrapsTheOldestReachableMachine(t *testing.T) {
	now := time.Now()
	tcp := newPreTerminateTCP()
	bf := newBootstrapFixture(t, tcp,
		newRMMachine(tcp, "m-newer", now.Add(-time.Hour)),
		newRMMachine(tcp, "m-older", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m-newest", now, bsNoAddress))

	result, err := bf.run()
	require.NoError(t, err)

	assert.True(t, result.IsZero())
	assert.Equal(t, []string{"m-older"}, bf.bootstrapped)
	assert.ElementsMatch(t, []string{"m-older", "m-newer"}, bf.asked, "every reachable machine is checked for etcd data first")
	bf.expectBootstrapped(t, "m-older")
	assert.Contains(t, strings.Join(bf.events(), "\n"), "ControlPlaneBootstrapped")
}

// A machine that already holds etcd data was bootstrapped, whatever the status says (lost on a
// controller crash between the request and the status patch, or on a clusterctl move): it is
// recorded and nothing is bootstrapped again.
func TestReconcileBootstrap_TakesAMachineHoldingEtcdDataAsBootstrapped(t *testing.T) {
	now := time.Now()
	tcp := newPreTerminateTCP()
	bf := newBootstrapFixture(t, tcp,
		newRMMachine(tcp, "m1", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m2", now.Add(-time.Hour)))
	bf.dataOn["m2"] = true

	result, err := bf.run()
	require.NoError(t, err)

	assert.True(t, result.IsZero())
	assert.Empty(t, bf.bootstrapped)
	bf.expectBootstrapped(t, "m2")
}

// Two bootstrapped machines are two etcd clusters: while a reachable machine cannot be asked
// whether it holds etcd data, nothing is bootstrapped.
func TestReconcileBootstrap_HoldsWhileAMachineCannotBeAsked(t *testing.T) {
	now := time.Now()
	tcp := newPreTerminateTCP()
	bf := newBootstrapFixture(t, tcp,
		newRMMachine(tcp, "m1", now.Add(-2*time.Hour)),
		newRMMachine(tcp, "m2", now.Add(-time.Hour)))
	bf.dataErr["m2"] = errors.New("connection refused")

	result, err := bf.run()
	require.NoError(t, err)

	bf.expectHeld(t, result, controlplanev1.WaitingForTalosBootReason, "Cannot tell whether a control plane machine already holds etcd data")
	assert.False(t, bf.tcp.Status.Bootstrapped)
}

func TestReconcileBootstrap_ReportsAFailedBootstrapRequest(t *testing.T) {
	tcp := newPreTerminateTCP()
	bf := newBootstrapFixture(t, tcp, newRMMachine(tcp, "m1", time.Now()))
	bf.bootstrapErr = errors.New("rpc error: code = Unavailable")

	result, err := bf.run()
	require.NoError(t, err)

	assert.Equal(t, bootstrapRetryAfter, result.RequeueAfter)
	assert.Equal(t, []string{"m1"}, bf.bootstrapped)
	assert.False(t, bf.tcp.Status.Bootstrapped)

	c := conditions.Get(bf.tcp, string(controlplanev1.MachinesBootstrapped))
	require.NotNil(t, c)
	assert.Equal(t, controlplanev1.WaitingForTalosBootReason, c.Reason)
	assert.Contains(t, c.Message, "Failed to bootstrap control plane machine m1")
}

func TestReconcileBootstrap_IsIdleOnceBootstrapped(t *testing.T) {
	now := time.Now()

	for name, tc := range map[string]struct {
		tcp      func() *controlplanev1.TalosControlPlane
		machines func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine
	}{
		"bootstrapped machine present": {
			tcp: func() *controlplanev1.TalosControlPlane {
				tcp := newPreTerminateTCP()
				bsBootstrapped(tcp, "m1")

				return tcp
			},
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newRMMachine(tcp, "m1", now)}
			},
		},
		// The lock is held for as long as the Machine exists, deleting included: it may still be
		// the only machine running etcd.
		"bootstrapped machine deleting": {
			tcp: func() *controlplanev1.TalosControlPlane {
				tcp := newPreTerminateTCP()
				bsBootstrapped(tcp, "m1")

				return tcp
			},
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{
					newRMMachine(tcp, "m1", now, ptDeleting(now, "")),
					newRMMachine(tcp, "m2", now),
				}
			},
		},
		// Once initialized, the regular etcd checks protect the cluster: the record only informs.
		"initialized and bootstrapped machine gone": {
			tcp: func() *controlplanev1.TalosControlPlane {
				tcp := newPreTerminateTCP()
				bsBootstrapped(tcp, "m0")
				tcp.Status.Initialization.ControlPlaneInitialized = new(true)

				return tcp
			},
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newRMMachine(tcp, "m1", now)}
			},
		},
		// A control plane bootstrapped before the machine was recorded has nothing to release.
		"legacy record without a machine": {
			tcp: func() *controlplanev1.TalosControlPlane {
				tcp := newPreTerminateTCP()
				tcp.Status.Bootstrapped = true

				return tcp
			},
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newRMMachine(tcp, "m1", now)}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			tcp := tc.tcp()
			bf := newBootstrapFixture(t, tcp, tc.machines(tcp)...)
			before := tcp.Status.DeepCopy()

			result, err := bf.run()
			require.NoError(t, err)

			assert.True(t, result.IsZero())
			assert.Empty(t, bf.bootstrapped)
			assert.Empty(t, bf.asked, "no node is consulted")
			assert.Equal(t, before.Bootstrapped, tcp.Status.Bootstrapped)
			assert.Equal(t, before.BootstrappedMachine, tcp.Status.BootstrappedMachine)
		})
	}
}

// The bootstrapped machine deleted before the control plane was initialized releases the lock,
// as the init machine's disappearance releases KCP's. Unlike KCP's joiners, Talos machines join
// etcd without waiting for initialization, so the etcd may live on elsewhere: that machine is
// recorded then, and only when no machine holds etcd data is another one bootstrapped.
func TestReconcileBootstrap_WhenTheBootstrappedMachineIsGone(t *testing.T) {
	now := time.Now()

	newTCP := func() *controlplanev1.TalosControlPlane {
		tcp := newPreTerminateTCP()
		bsBootstrapped(tcp, "m-gone")

		return tcp
	}

	t.Run("records the machine holding etcd data", func(t *testing.T) {
		tcp := newTCP()
		bf := newBootstrapFixture(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m2", now.Add(-time.Hour)))
		bf.dataOn["m2"] = true

		result, err := bf.run()
		require.NoError(t, err)

		assert.True(t, result.IsZero())
		assert.Empty(t, bf.bootstrapped)
		bf.expectBootstrapped(t, "m2")

		events := strings.Join(bf.events(), "\n")
		assert.Contains(t, events, "BootstrappedMachineGone")
		assert.Contains(t, events, "Machine m2 holds the etcd data")
	})

	t.Run("bootstraps the oldest machine when none holds etcd data", func(t *testing.T) {
		tcp := newTCP()
		bf := newBootstrapFixture(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m2", now.Add(-time.Hour)))

		result, err := bf.run()
		require.NoError(t, err)

		assert.True(t, result.IsZero())
		assert.Equal(t, []string{"m1"}, bf.bootstrapped)
		bf.expectBootstrapped(t, "m1")
		assert.Contains(t, strings.Join(bf.events(), "\n"), "bootstrapping another machine")
	})

	t.Run("waits for every machine to report an address", func(t *testing.T) {
		tcp := newTCP()
		bf := newBootstrapFixture(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m2", now.Add(-time.Hour), bsNoAddress))

		result, err := bf.run()
		require.NoError(t, err)

		bf.expectHeld(t, result, controlplanev1.BootstrappedMachineGoneReason, "waiting for machines to report an address")
		assert.True(t, bf.tcp.Status.Bootstrapped, "the lock is not released")
		assert.Equal(t, "m-gone", bf.tcp.Status.BootstrappedMachine)
		assert.Empty(t, bf.asked)
	})

	t.Run("waits while a machine cannot be asked", func(t *testing.T) {
		tcp := newTCP()
		bf := newBootstrapFixture(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m2", now.Add(-time.Hour)))
		bf.dataErr["m1"] = errors.New("connection refused")

		result, err := bf.run()
		require.NoError(t, err)

		bf.expectHeld(t, result, controlplanev1.BootstrappedMachineGoneReason, "cannot tell whether a remaining machine holds the etcd data")
		assert.True(t, bf.tcp.Status.Bootstrapped, "the lock is not released")
	})

	t.Run("ignores a deleting machine without an address", func(t *testing.T) {
		tcp := newTCP()
		bf := newBootstrapFixture(t, tcp,
			newRMMachine(tcp, "m1", now.Add(-2*time.Hour)),
			newRMMachine(tcp, "m2", now.Add(-time.Hour), bsNoAddress, ptDeleting(now, "")))

		result, err := bf.run()
		require.NoError(t, err)

		assert.True(t, result.IsZero())
		assert.Equal(t, []string{"m1"}, bf.bootstrapped)
	})
}

func TestBootstrapCandidates(t *testing.T) {
	now := time.Now()
	tcp := newPreTerminateTCP()

	candidates := bootstrapCandidates([]clusterv1.Machine{
		*newRMMachine(tcp, "b-newer", now.Add(-time.Hour)),
		*newRMMachine(tcp, "deleting", now.Add(-3*time.Hour), ptDeleting(now, "")),
		*newRMMachine(tcp, "a-newer", now.Add(-time.Hour)),
		*newRMMachine(tcp, "no-address", now.Add(-4*time.Hour), bsNoAddress),
		*newRMMachine(tcp, "oldest", now.Add(-2*time.Hour)),
	})

	names := make([]string, 0, len(candidates))
	for _, c := range candidates {
		names = append(names, c.Name)
	}

	assert.Equal(t, []string{"oldest", "a-newer", "b-newer"}, names, "oldest first, names break ties, deleting and unreachable machines left out")
}

func TestMachineBootstrapAddress(t *testing.T) {
	m := clusterv1.Machine{Status: clusterv1.MachineStatus{Addresses: clusterv1.MachineAddresses{
		{Type: clusterv1.MachineHostName, Address: "cp-1"},
		{Type: clusterv1.MachineExternalIP, Address: "203.0.113.10"},
		{Type: clusterv1.MachineInternalIP, Address: "10.0.0.10"},
	}}}
	assert.Equal(t, "10.0.0.10", machineBootstrapAddress(m), "an internal address is preferred")

	m.Status.Addresses = m.Status.Addresses[:2]
	assert.Equal(t, "203.0.113.10", machineBootstrapAddress(m))

	m.Status.Addresses = m.Status.Addresses[:1]
	assert.Empty(t, machineBootstrapAddress(m), "a host name is not an endpoint")
}

// Like KCP, a control plane whose only machine is on its way out is not reported initialized,
// even if its API server still answers: the replacement could never bootstrap afterwards.
func TestInitializationOnHold(t *testing.T) {
	now := time.Now()
	tcp := newPreTerminateTCP()

	assert.False(t, initializationOnHold(nil))
	assert.False(t, initializationOnHold([]clusterv1.Machine{*newRMMachine(tcp, "m1", now)}))
	assert.True(t, initializationOnHold([]clusterv1.Machine{*newRMMachine(tcp, "m1", now, ptDeleting(now, ""))}))
	assert.True(t, initializationOnHold([]clusterv1.Machine{*newRMMachine(tcp, "m1", now, rmFlagged)}))
	assert.False(t, initializationOnHold([]clusterv1.Machine{
		*newRMMachine(tcp, "m1", now, ptDeleting(now, "")),
		*newRMMachine(tcp, "m2", now),
	}), "with more than one machine the regular checks apply")
}
