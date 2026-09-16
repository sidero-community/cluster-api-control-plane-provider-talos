// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

// Unexported reconciler steps exposed to the external test package.
var (
	BootstrapCluster = (*TalosControlPlaneReconciler).bootstrapCluster
	EtcdHealthcheck  = (*TalosControlPlaneReconciler).etcdHealthcheck
)
