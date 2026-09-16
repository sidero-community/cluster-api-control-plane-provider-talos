// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers_test

import (
	. "github.com/onsi/gomega"
	"github.com/siderolabs/talos/pkg/machinery/api/machine"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util"

	"github.com/siderolabs/cluster-api-control-plane-provider-talos/controllers"
)

// TestBootstrapSkipsAClusterWithEtcdData: a node that already has an etcd data directory
// means the cluster was bootstrapped, so Bootstrap must not be sent again.
func (suite *ControllersSuite) TestBootstrapSkipsAClusterWithEtcdData() {
	g := NewWithT(suite.T())

	fakeClient := newFakeClient()
	cluster, tcp, _ := suite.setupCluster(fakeClient, "test-bootstrap-skip", nil)
	r := newReconciler(fakeClient, withCluster(util.ObjectKey(cluster)))

	machineService, machineAddress := suite.startMachineServer()
	machineService.setListEntries(&machine.FileInfo{Name: "/var/lib/etcd/member", IsDir: true})

	m, n := createMachineNodePair("bootstrapped-machine", cluster, tcp, true, machineAddress)
	g.Expect(fakeClient.Create(suite.ctx, m)).To(Succeed())
	g.Expect(fakeClient.Create(suite.ctx, n)).To(Succeed())
	g.Expect(createSecrets(suite.ctx, fakeClient, cluster, suite.secretsBundle, machineAddress)).To(Succeed())

	g.Expect(controllers.BootstrapCluster(r, suite.ctx, tcp, []clusterv1.Machine{*m})).To(Succeed())
	g.Expect(machineService.getBootstrapCalls()).To(BeZero())
}

// TestBootstrapBootstrapsAClusterWithoutEtcdData: with no etcd data on any node, Bootstrap is
// sent exactly once.
func (suite *ControllersSuite) TestBootstrapBootstrapsAClusterWithoutEtcdData() {
	g := NewWithT(suite.T())

	fakeClient := newFakeClient()
	cluster, tcp, _ := suite.setupCluster(fakeClient, "test-bootstrap-fresh", nil)
	r := newReconciler(fakeClient, withCluster(util.ObjectKey(cluster)))

	machineService, machineAddress := suite.startMachineServer()

	m, n := createMachineNodePair("fresh-machine", cluster, tcp, true, machineAddress)
	g.Expect(fakeClient.Create(suite.ctx, m)).To(Succeed())
	g.Expect(fakeClient.Create(suite.ctx, n)).To(Succeed())
	g.Expect(createSecrets(suite.ctx, fakeClient, cluster, suite.secretsBundle, machineAddress)).To(Succeed())

	g.Expect(controllers.BootstrapCluster(r, suite.ctx, tcp, []clusterv1.Machine{*m})).To(Succeed())
	g.Expect(machineService.getBootstrapCalls()).To(Equal(1))
}

// TestEtcdHealthcheckNamesTheMachine: a failing etcd check reports the Machine it was run
// against, without relying on the hostname metadata Talos no longer sends.
func (suite *ControllersSuite) TestEtcdHealthcheckNamesTheMachine() {
	g := NewWithT(suite.T())

	fakeClient := newFakeClient()
	cluster, tcp, _ := suite.setupCluster(fakeClient, "test-etcd-health-name", nil)
	r := newReconciler(fakeClient, withCluster(util.ObjectKey(cluster)))

	machineService, machineAddress := suite.startMachineServer()
	machineService.setServiceListResponse(&machine.ServiceListResponse{
		Messages: []*machine.ServiceList{{
			Services: []*machine.ServiceInfo{{Id: "etcd", State: "Preparing", Events: &machine.ServiceEvents{}}},
		}},
	})

	m, n := createMachineNodePair("quiet-machine", cluster, tcp, true, machineAddress)
	g.Expect(fakeClient.Create(suite.ctx, m)).To(Succeed())
	g.Expect(fakeClient.Create(suite.ctx, n)).To(Succeed())
	g.Expect(createSecrets(suite.ctx, fakeClient, cluster, suite.secretsBundle, machineAddress)).To(Succeed())

	err := controllers.EtcdHealthcheck(r, suite.ctx, tcp, []clusterv1.Machine{*m})
	g.Expect(err).To(MatchError(ContainSubstring("quiet-machine: no events recorded yet")))
}
