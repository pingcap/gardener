// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package botanist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	gardencorev1beta1helper "github.com/gardener/gardener/pkg/apis/core/v1beta1/helper"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/gardener/gardener/pkg/controllerutils"
	"github.com/gardener/gardener/pkg/operation/common"
	"github.com/gardener/gardener/pkg/utils/secrets"
	errors2 "k8s.io/apimachinery/pkg/api/errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// InfrastructureDefaultTimeout is the default timeout and defines how long Gardener should wait
// for a successful reconciliation of an infrastructure resource.
const InfrastructureDefaultTimeout = 5 * time.Minute

// DeployInfrastructure creates the `Infrastructure` extension resource in the shoot namespace in the seed
// cluster. Gardener waits until an external controller did reconcile the cluster successfully.
func (b *Botanist) DeployInfrastructure(ctx context.Context) error {
	var (
		lastOperation                       = b.Shoot.Info.Status.LastOperation
		creationPhase                       = lastOperation != nil && lastOperation.Type == gardencorev1beta1.LastOperationTypeCreate
		shootIsWakingUp                     = !gardencorev1beta1helper.HibernationIsEnabled(b.Shoot.Info) && b.Shoot.Info.Status.IsHibernated
		requestInfrastructureReconciliation = creationPhase || shootIsWakingUp || controllerutils.HasTask(b.Shoot.Info.Annotations, common.ShootTaskDeployInfrastructure)

		infrastructure = &extensionsv1alpha1.Infrastructure{
			ObjectMeta: metav1.ObjectMeta{
				Name:      b.Shoot.Info.Name,
				Namespace: b.Shoot.SeedNamespace,
			},
		}
		providerConfig *runtime.RawExtension
	)

	if cfg := b.Shoot.Info.Spec.Provider.InfrastructureConfig; cfg != nil {
		providerConfig = &runtime.RawExtension{
			Raw: cfg.Raw,
		}
	}

	_, err := controllerutil.CreateOrUpdate(ctx, b.K8sSeedClient.Client(), infrastructure, func() error {
		if requestInfrastructureReconciliation {
			metav1.SetMetaDataAnnotation(&infrastructure.ObjectMeta, v1beta1constants.GardenerOperation, v1beta1constants.GardenerOperationReconcile)
		}

		infrastructure.Labels["tenant"] = b.Shoot.Info.Labels["tenant"]
		infrastructure.Spec = extensionsv1alpha1.InfrastructureSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type:           b.Shoot.Info.Spec.Provider.Type,
				ProviderConfig: providerConfig,
			},
			Region:       b.Shoot.Info.Spec.Region,
			SSHPublicKey: b.Secrets[v1beta1constants.SecretNameSSHKeyPair].Data[secrets.DataKeySSHAuthorizedKeys],
			SecretRef: corev1.SecretReference{
				Name:      v1beta1constants.SecretNameCloudProvider,
				Namespace: infrastructure.Namespace,
			},
		}
		return nil
	})
	return err
}

// DestroyInfrastructure deletes the `Infrastructure` extension resource in the shoot namespace in the seed cluster,
// and it waits for a maximum of 10m until it is deleted.
func (b *Botanist) DestroyInfrastructure(ctx context.Context) error {
	return common.DeleteExtensionCR(
		ctx,
		b.K8sSeedClient.Client(),
		func() extensionsv1alpha1.Object { return &extensionsv1alpha1.Infrastructure{} },
		b.Shoot.SeedNamespace,
		b.Shoot.Info.Name,
	)
}

// WaitUntilInfrastructureReady waits until the infrastructure resource has been reconciled successfully.
func (b *Botanist) WaitUntilInfrastructureReady(ctx context.Context) error {
	return common.WaitUntilExtensionCRReady(
		ctx,
		b.K8sSeedClient.Client(),
		b.Logger,
		func() runtime.Object { return &extensionsv1alpha1.Infrastructure{} },
		"Infrastructure",
		b.Shoot.SeedNamespace,
		b.Shoot.Info.Name,
		DefaultInterval,
		InfrastructureDefaultTimeout,
		func(obj runtime.Object) error {
			infrastructure, ok := obj.(*extensionsv1alpha1.Infrastructure)
			if !ok {
				return fmt.Errorf("expected extensionsv1alpha1.Infrastructure but got %T", infrastructure)
			}

			if infrastructure.Status.ProviderStatus != nil {
				b.Shoot.InfrastructureStatus = infrastructure.Status.ProviderStatus.Raw
			}

			if infrastructure.Status.NodesCIDR != nil {
				shootCopy := b.Shoot.Info.DeepCopy()
				if _, err := controllerutil.CreateOrUpdate(ctx, b.K8sGardenClient.Client(), shootCopy, func() error {
					shootCopy.Spec.Networking.Nodes = infrastructure.Status.NodesCIDR
					return nil
				}); err != nil {
					return err
				}
				b.Shoot.Info = shootCopy
			}
			return nil
		},
	)
}

// DeployApiServerFirewall add the shoot worker nodes as a trust source of shoot apiserver
func (b *Botanist) DeployApiServerFirewall(ctx context.Context) error {
	var sourceRanges []string
	if len(b.Shoot.Info.Spec.LoadBalancerSourceRanges) > 0 {
		sourceRanges = append(sourceRanges, b.Shoot.Info.Spec.LoadBalancerSourceRanges...)

		cm, err := b.K8sSeedClient.Kubernetes().CoreV1().ConfigMaps("kube-system").Get("vpc-outbound-ips", metav1.GetOptions{})
		if err != nil && !errors2.IsNotFound(err) {
			return err
		}

		// Add seed outbound ips, so that watchdog probing could success
		if err == nil && len(cm.Data["ips"]) > 0 {
			ipStr := cm.Data["ips"]
			ipList := strings.Split(ipStr, " ")
			for _, ip := range ipList {
				sourceRanges = append(sourceRanges, fmt.Sprintf("%s/32", ip))
			}
		}
	} else {
		// use a safe default if the source ranges are not explicitly set
		sourceRanges = append(sourceRanges, "0.0.0.0/0")
	}

	// By design, this action should create a custom resource to describe the intention and
	// let the cloud provider extension to handle the implementation details.
	// TODO: refactor this using CR
	if b.Shoot.Info.Spec.Provider.Type == "aws" {
		workerIps, err := b.getAwsWorkerPublicIPs(ctx)
		if err != nil {
			return err
		}
		for _, v := range workerIps {
			sourceRanges = append(sourceRanges, fmt.Sprintf("%s/32", v))
		}
	}

	svc, err := b.K8sSeedClient.Kubernetes().CoreV1().Services(b.Shoot.SeedNamespace).Get("kube-apiserver", metav1.GetOptions{})
	if err != nil {
		return err
	}
	svc.Spec.LoadBalancerSourceRanges = sourceRanges
	return b.K8sSeedClient.Client().Update(ctx, svc)
}

func (b *Botanist) getAwsWorkerPublicIPs(ctx context.Context) ([]string, error) {
	type WithNatIps struct {
		VPC struct {
			NatIPs []string `json:"natIps"`
		} `json:"vpc"`
	}
	if b.Shoot.InfrastructureStatus == nil {
		return nil, errors.New("cannot find infra status")
	}
	var s WithNatIps
	err := json.Unmarshal(b.Shoot.InfrastructureStatus, &s)
	if err != nil {
		return nil, err
	}
	if s.VPC.NatIPs == nil {
		b.Logger.Info("Cannot find nat ips, infra may be reconciled by legacy extension controller")
		return nil, nil
	}
	return s.VPC.NatIPs, nil
}

// WaitUntilInfrastructureDeleted waits until the infrastructure resource has been deleted.
func (b *Botanist) WaitUntilInfrastructureDeleted(ctx context.Context) error {
	return common.WaitUntilExtensionCRDeleted(
		ctx,
		b.K8sSeedClient.Client(),
		b.Logger,
		func() extensionsv1alpha1.Object { return &extensionsv1alpha1.Infrastructure{} },
		"Infrastructure",
		b.Shoot.SeedNamespace,
		b.Shoot.Info.Name,
		DefaultInterval,
		InfrastructureDefaultTimeout,
	)
}
