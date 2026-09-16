// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package ssa_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/siderolabs/cluster-api-control-plane-provider-talos/internal/ssa"
)

func TestPatchCreatesAndOwnsTheObject(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"},
		Data:       map[string]string{"k": "v"},
	}

	require.NoError(t, ssa.Patch(context.Background(), c, cm))

	stored := &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "cfg"}, stored))
	assert.Equal(t, "v", stored.Data["k"])
	assert.NotEmpty(t, stored.ResourceVersion, "the server's result is copied back")
}

// capturedApplyOptions intercepts Apply and records the options the helper passed, since the
// fake client stores dry-run applies like real ones.
func capturedApplyOptions(t *testing.T, patch func(context.Context, client.Client, client.Object) error) client.ApplyOptions {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	var captured client.ApplyOptions

	c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Apply: func(_ context.Context, _ client.WithWatch, _ runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			captured.ApplyOptions(opts)

			return nil
		},
	}).Build()

	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"},
	}

	require.NoError(t, patch(context.Background(), c, cm))

	return captured
}

func TestPatchAppliesAsTheOwningManagerWithForce(t *testing.T) {
	t.Parallel()

	opts := capturedApplyOptions(t, ssa.Patch)

	assert.Equal(t, ssa.ManagerName, opts.FieldManager)
	assert.True(t, ptr.Deref(opts.Force, false))
	assert.Empty(t, opts.DryRun)
}

func TestDryRunPatchAppliesWithDryRunAll(t *testing.T) {
	t.Parallel()

	opts := capturedApplyOptions(t, ssa.DryRunPatch)

	assert.Equal(t, ssa.ManagerName, opts.FieldManager)
	assert.True(t, ptr.Deref(opts.Force, false))
	assert.Equal(t, []string{metav1.DryRunAll}, opts.DryRun)
}
