// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	"google.golang.org/grpc/codes"
	corev1 "k8s.io/api/core/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

type errServiceUnhealthy struct {
	service string
	reason  string
}

func (e *errServiceUnhealthy) Error() string {
	return fmt.Sprintf("Service %s is unhealthy: %s", e.service, e.reason)
}

// healthCheckable narrows machines to the ones whose services are expected to be healthy:
// not deleting, and not marked as leaving etcd. A machine on its way out has a drained node
// and possibly no etcd member anymore, so counting it would only make the health conditions
// flap through every deletion.
func healthCheckable(machines []clusterv1.Machine) []clusterv1.Machine {
	out := make([]clusterv1.Machine, 0, len(machines))

	for _, machine := range machines {
		if !machine.DeletionTimestamp.IsZero() || machine.Annotations[etcdLeavingAnnotation] == "true" {
			continue
		}

		out = append(out, machine)
	}

	return out
}

// staticPodStatus is what the health check needs from the kubelet's status of a static pod, as
// Talos records it: the pod's "<namespace>/<name>", and whether it is Ready.
type staticPodStatus struct {
	id    string
	ready bool
}

// controlPlaneComponents are the static pods Talos renders on a control plane node. The
// kubelet names the pod it runs from each "<component>-<node name>".
var controlPlaneComponents = []string{k8s.APIServerID, k8s.ControllerManagerID, k8s.SchedulerID}

// controlPlaneComponentsError reports the first control plane component whose static pod the
// kubelet does not run ready. Like KCP's control plane health check, every component has to be
// there and Ready: Talos restarts kube-apiserver itself when the configuration changes what it
// runs with, and a configuration it cannot start on is only visible here, not on the Talos
// services.
func controlPlaneComponentsError(statuses []staticPodStatus) error {
	for _, component := range controlPlaneComponents {
		prefix := "kube-system/" + component + "-"
		found := false

		for _, status := range statuses {
			if !strings.HasPrefix(status.id, prefix) {
				continue
			}

			found = true

			if !status.ready {
				return &errServiceUnhealthy{service: component, reason: "static pod is not ready"}
			}
		}

		if !found {
			return &errServiceUnhealthy{service: component, reason: "static pod not found"}
		}
	}

	return nil
}

// staticPodReady reads the Ready condition out of the PodStatus Talos stores on a
// StaticPodStatus resource, which is the kubelet's PodStatus as a generic map.
func staticPodReady(podStatus map[string]any) (bool, error) {
	encoded, err := json.Marshal(podStatus)
	if err != nil {
		return false, fmt.Errorf("failed to encode the pod status: %w", err)
	}

	var status corev1.PodStatus

	if err := json.Unmarshal(encoded, &status); err != nil {
		return false, fmt.Errorf("failed to decode the pod status: %w", err)
	}

	for _, condition := range status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue, nil
		}
	}

	return false, nil
}

// nodesHealthcheck fails on the first machine reporting a service, or a control plane static
// pod, that is known to be unhealthy. Callers pass the machines they expect to be healthy (see
// healthCheckable).
func (r *TalosControlPlaneReconciler) nodesHealthcheck(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	for _, machine := range machines {
		if err := func() error {
			client, err := r.etcdClientFor(ctx, tcp, machine)
			if err != nil {
				return err
			}

			defer client.Close() //nolint:errcheck

			serviceList, err := client.ServiceList(ctx)
			if err != nil {
				return err
			}

			for _, message := range serviceList.Messages {
				for _, svc := range message.Services {
					if !svc.GetHealth().Unknown && !svc.GetHealth().Healthy {
						return &errServiceUnhealthy{
							service: svc.GetId(),
							reason:  svc.GetState(),
						}
					}
				}
			}

			// The Kubernetes control plane components are static pods the kubelet runs, not
			// Talos services: their health is the kubelet's view of them.
			statuses, err := client.StaticPodStatuses(ctx)
			if err != nil {
				return err
			}

			return controlPlaneComponentsError(statuses)
		}(); err != nil {
			return fmt.Errorf("machine %q: %w", machine.Name, err)
		}
	}

	return nil
}

func (r *TalosControlPlaneReconciler) ensureNodesBooted(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	eventsCh := make(chan talosclient.EventResult)

	var clients []*talosclient.Client

	defer func() {
		for _, client := range clients {
			client.Close() //nolint:errcheck
		}
	}()

	// watch events from all machines into a single channel
	for _, machine := range machines {
		client, err := r.talosconfigForMachines(ctx, tcp, machine)
		if err != nil {
			return err
		}

		clients = append(clients, client)

		if err := client.EventsWatchV2(ctx, eventsCh, talosclient.WithTailEvents(-1)); err != nil {
			return err
		}
	}

	nodesBootStarted := map[string]struct{}{}
	nodesBootStopped := map[string]struct{}{}

loop:
	for {
		var eventResult talosclient.EventResult

		select {
		case <-ctx.Done():
			// timeout
			break loop
		case eventResult = <-eventsCh:
		}

		if eventResult.Error != nil {
			switch {
			case talosclient.StatusCode(eventResult.Error) == codes.DeadlineExceeded:
				// expected, we've exhausted events list
			case errors.Is(eventResult.Error, context.Canceled):
				// expected, fine
			default:
				return fmt.Errorf("failed to watch events: %w", eventResult.Error)
			}
		} else {
			event := eventResult.Event

			if msg, ok := event.Payload.(*machineapi.SequenceEvent); ok {
				if msg.GetSequence() == "boot" { // can't use runtime constants as they're in `internal/`
					switch msg.GetAction() { //nolint:exhaustive
					case machineapi.SequenceEvent_START:
						nodesBootStarted[event.Node] = struct{}{}
					case machineapi.SequenceEvent_STOP:
						nodesBootStopped[event.Node] = struct{}{}
					}
				}
			}
		}
	}

	nodesNotFinishedBooting := []string{}

	// check for nodes which have Boot/Start event, but no Boot/Stop even
	// if the node is up long enough, Boot/Start even might get out of the window,
	// so we can't check such nodes reliably
	for node := range nodesBootStarted {
		if _, ok := nodesBootStopped[node]; !ok {
			nodesNotFinishedBooting = append(nodesNotFinishedBooting, node)
		}
	}

	sort.Strings(nodesNotFinishedBooting)

	if len(nodesNotFinishedBooting) > 0 {
		return fmt.Errorf("nodes %q are still in boot sequence", nodesNotFinishedBooting)
	}

	return nil
}
