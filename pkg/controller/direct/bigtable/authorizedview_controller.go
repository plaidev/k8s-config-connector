// Copyright 2025 Google LLC
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

package bigtable

import (
	"context"
	"fmt"
	"strings"

	iam "cloud.google.com/go/iam"
	"cloud.google.com/go/iam/apiv1/iampb"
	krm "github.com/GoogleCloudPlatform/k8s-config-connector/apis/bigtable/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-config-connector/pkg/config"
	"github.com/GoogleCloudPlatform/k8s-config-connector/pkg/controller/direct"
	"github.com/GoogleCloudPlatform/k8s-config-connector/pkg/controller/direct/directbase"
	"github.com/GoogleCloudPlatform/k8s-config-connector/pkg/controller/direct/registry"

	gcp "cloud.google.com/go/bigtable"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

func init() {
	registry.RegisterModel(krm.BigtableAuthorizedViewGVK, NewBigtableAuthorizedViewModel)
}

func NewBigtableAuthorizedViewModel(ctx context.Context, config *config.ControllerConfig) (directbase.Model, error) {
	return &modelBigtableAuthorizedView{config: *config}, nil
}

var _ directbase.Model = &modelBigtableAuthorizedView{}

type modelBigtableAuthorizedView struct {
	config config.ControllerConfig
}

func (m *modelBigtableAuthorizedView) client(ctx context.Context, parentProject string, instanceID string) (*gcp.AdminClient, error) {
	var opts []option.ClientOption
	opts, err := m.config.GRPCClientOptions()
	if err != nil {
		return nil, fmt.Errorf("getting GRPC client options: %w", err)
	}
	gcpClient, err := gcp.NewAdminClient(ctx, parentProject, instanceID, opts...)
	if err != nil {
		return nil, fmt.Errorf("building BigtableAuthorizedView client: %w", err)
	}
	return gcpClient, nil
}

// This helper function converts a fully qualified project like "projects/myproject" into
// the unqualified project ID, like "myproject".
func (m *modelBigtableAuthorizedView) getProjectId(fullyQualifiedProject string) (string, error) {
	tokens := strings.Split(fullyQualifiedProject, "/")
	if len(tokens) != 2 || tokens[0] != "projects" {
		return "", fmt.Errorf("Unexpected format for AuthorizedView Parent Project ID=%q was not known (expected projects/{projectID})", fullyQualifiedProject)
	}
	return tokens[1], nil
}

func (m *modelBigtableAuthorizedView) AdapterForObject(ctx context.Context, op *directbase.AdapterForObjectOperation) (directbase.Adapter, error) {
	reader := op.Reader
	u := op.GetUnstructured()

	obj := &krm.BigtableAuthorizedView{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &obj); err != nil {
		return nil, fmt.Errorf("error converting to %T: %w", obj, err)
	}

	id, err := krm.NewAuthorizedViewIdentity(ctx, reader, obj)
	if err != nil {
		return nil, err
	}

	// Get bigtable admin GCP client. Accepts the non-fully qualified project ID.
	// E.G. "myproject" instead of "projects/myproject"
	parentProjectId, err := m.getProjectId(id.Parent().Parent.ParentString())
	if err != nil {
		return nil, err
	}
	instanceId := id.Parent().Parent.Id
	adminClient, err := m.client(ctx, parentProjectId, instanceId)
	if err != nil {
		return nil, fmt.Errorf("error creating admin client: %w", err)
	}
	return &BigtableAuthorizedViewAdapter{
		id:        id,
		gcpClient: adminClient,
		desired:   obj,
	}, nil
}

func (m *modelBigtableAuthorizedView) AdapterForURL(ctx context.Context, url string) (directbase.Adapter, error) {
	// Format is //bigtable.googleapis.com/projects/PROJECT_ID/instances/INSTANCE_ID/tables/TABLE_ID/authorizedViews/AUTHORIZED_VIEW_ID

	fmt.Printf("AdapterUrl=%s\n", url)
	if !strings.HasPrefix(url, "//bigtable.googleapis.com/") {
		return nil, nil
	}

	tokens := strings.Split(strings.TrimPrefix(url, "//bigtable.googleapis.com/"), "/")
	if len(tokens) == 8 && tokens[0] == "projects" && tokens[2] == "instances" && tokens[4] == "tables" && tokens[6] == "authorizedViews" {
		projectID := tokens[1]
		instanceID := tokens[3]
		avClient, err := m.client(ctx, projectID, instanceID)
		if err != nil {
			return nil, err
		}
		id, err := krm.NewAuthorizedViewIdentityFromExternal(strings.TrimPrefix(url, "//bigtable.googleapis.com/"))
		if err != nil {
			return nil, err
		}

		return &BigtableAuthorizedViewAdapter{
			id:        id,
			gcpClient: avClient,
		}, nil
	}

	return nil, nil
}

type BigtableAuthorizedViewAdapter struct {
	id        *krm.AuthorizedViewIdentity
	gcpClient *gcp.AdminClient
	desired   *krm.BigtableAuthorizedView
	actual    *gcp.AuthorizedViewInfo
}

var _ directbase.Adapter = &BigtableAuthorizedViewAdapter{}

// Find retrieves the GCP resource.
// Return true means the object is found. This triggers Adapter `Update` call.
// Return false means the object is not found. This triggers Adapter `Create` call.
// Return a non-nil error requeues the requests.
func (a *BigtableAuthorizedViewAdapter) Find(ctx context.Context) (bool, error) {
	log := klog.FromContext(ctx)
	log.V(2).Info("getting BigtableAuthorizedView", "name", a.id)

	bigtableauthorizedview, err := a.gcpClient.AuthorizedViewInfo(ctx, a.id.Parent().ID(), a.id.ID())
	if err != nil {
		if direct.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting BigtableAuthorizedView %q: %w", a.id, err)
	}

	a.actual = bigtableauthorizedview
	return true, nil
}

// Create creates the resource in GCP based on `spec` and update the Config Connector object `status` based on the GCP response.
func (a *BigtableAuthorizedViewAdapter) Create(ctx context.Context, createOp *directbase.CreateOperation) error {
	log := klog.FromContext(ctx)
	log.V(2).Info("creating BigtableAuthorizedView", "name", a.id)
	mapCtx := &direct.MapContext{}

	desired := a.desired.DeepCopy()
	resource := BigtableAuthorizedViewSpec_ToProto(mapCtx, &desired.Spec)
	if mapCtx.Err() != nil {
		return mapCtx.Err()
	}

	// Set the name field
	resource.Name = a.id.String()

	familySubset := make(map[string]gcp.FamilySubset)
	for key, ptrValue := range resource.GetSubsetView().FamilySubsets {
		if ptrValue != nil {
			familySubset[key] = gcp.FamilySubset{
				Qualifiers:        ptrValue.Qualifiers,
				QualifierPrefixes: ptrValue.QualifierPrefixes,
			}
		}
	}

	rowPrefix := [][]byte{[]byte("")}
	if resource.GetSubsetView().RowPrefixes != nil {
		rowPrefix = resource.GetSubsetView().RowPrefixes
	}

	subsetConf := &gcp.SubsetViewConf{
		RowPrefixes:   rowPrefix,
		FamilySubsets: familySubset,
	}
	conf := &gcp.AuthorizedViewConf{
		TableID:            a.id.Parent().ID(),
		AuthorizedViewID:   a.id.ID(),
		AuthorizedView:     subsetConf,
		DeletionProtection: 0,
	}

	// Create the authorized view
	err := a.gcpClient.CreateAuthorizedView(ctx, conf)
	if err != nil {
		return fmt.Errorf("creating BigtableAuthorizedView %s: %w", a.id, err)
	}
	log.V(2).Info("successfully created BigtableAuthorizedView", "name", a.id)

	status := &krm.BigtableAuthorizedViewStatus{}
	// TODO: Add Observed State
	// status.ObservedState = BigtableAuthorizedViewObservedState_FromProto(mapCtx, created)
	// if mapCtx.Err() != nil {
	// 	return mapCtx.Err()
	// }
	status.ExternalRef = direct.LazyPtr(a.id.String())
	return createOp.UpdateStatus(ctx, status, nil)
}

// Update updates the resource in GCP based on `spec` and update the Config Connector object `status` based on the GCP response.
func (a *BigtableAuthorizedViewAdapter) Update(ctx context.Context, updateOp *directbase.UpdateOperation) error {
	log := klog.FromContext(ctx)
	log.V(2).Info("updating BigtableAuthorizedView", "name", a.id)
	mapCtx := &direct.MapContext{}

	desired := a.desired.DeepCopy()
	resource := BigtableAuthorizedViewSpec_ToProto(mapCtx, &a.desired.Spec)
	if mapCtx.Err() != nil {
		return mapCtx.Err()
	}

	// Set the name field
	resource.Name = a.id.String()

	// Check for changes
	hasChanges := false

	familySubset := make(map[string]gcp.FamilySubset)
	for key, ptrValue := range resource.GetSubsetView().FamilySubsets {
		if ptrValue != nil {
			familySubset[key] = gcp.FamilySubset{
				Qualifiers:        ptrValue.Qualifiers,
				QualifierPrefixes: ptrValue.QualifierPrefixes,
			}
		}
	}

	rowPrefixes := resource.GetSubsetView().RowPrefixes
	if rowPrefixes == nil {
		rowPrefixes = [][]byte{[]byte("")}
	}
	subsetView := &gcp.SubsetViewInfo{
		RowPrefixes:   rowPrefixes,
		FamilySubsets: familySubset,
	}

	// Compare SubsetView
	if desired.Spec.SubsetView != nil && !cmp.Equal(subsetView, a.actual.AuthorizedView) {
		hasChanges = true
	}

	// TODO: Compare DeletionProtection
	//if desired.Spec.DeletionProtection != nil && !cmp.Equal(resource.GetDeletionProtection(), a.actual.DeletionProtection()) {
	//	hasChanges = true
	//}

	if !hasChanges {
		log.V(2).Info("no changes to update", "name", a.id)
	} else {
		// Create update mask
		paths := []string{}
		if desired.Spec.SubsetView != nil {
			paths = append(paths, "subset_view")
		}
		if desired.Spec.DeletionProtection != nil {
			paths = append(paths, "deletion_protection")
		}

		familySubset := make(map[string]gcp.FamilySubset)
		for key, ptrValue := range resource.GetSubsetView().FamilySubsets {
			if ptrValue != nil {
				familySubset[key] = gcp.FamilySubset{
					Qualifiers:        ptrValue.Qualifiers,
					QualifierPrefixes: ptrValue.QualifierPrefixes,
				}
			}
		}

		rowPrefix := [][]byte{[]byte("")}
		if resource.GetSubsetView().RowPrefixes != nil {
			rowPrefix = resource.GetSubsetView().RowPrefixes
		}

		subsetConf := &gcp.SubsetViewConf{
			RowPrefixes:   rowPrefix,
			FamilySubsets: familySubset,
		}
		conf := &gcp.AuthorizedViewConf{
			TableID:            a.id.Parent().ID(),
			AuthorizedViewID:   a.id.ID(),
			AuthorizedView:     subsetConf,
			DeletionProtection: 0,
		}
		updateConf := gcp.UpdateAuthorizedViewConf{
			AuthorizedViewConf: *conf,
			IgnoreWarnings:     true,
		}
		err := a.gcpClient.UpdateAuthorizedView(ctx, updateConf)
		if err != nil {
			return fmt.Errorf("updating BigtableAuthorizedView %s: %w", a.id, err)
		}
		log.V(2).Info("successfully updated BigtableAuthorizedView", "name", a.id)
	}

	status := &krm.BigtableAuthorizedViewStatus{}
	// TODO: Add ObservedState
	// status.ObservedState = BigtableAuthorizedViewObservedState_FromProto(mapCtx, updated)
	// if mapCtx.Err() != nil {
	// 	return mapCtx.Err()
	// }
	status.ExternalRef = direct.LazyPtr(a.id.String())
	return updateOp.UpdateStatus(ctx, status, nil)
}

// Export maps the GCP object to a Config Connector resource `spec`.
func (a *BigtableAuthorizedViewAdapter) Export(ctx context.Context) (*unstructured.Unstructured, error) {
	if a.actual == nil {
		return nil, fmt.Errorf("Find() not called")
	}
	u := &unstructured.Unstructured{}

	obj := &krm.BigtableAuthorizedView{}
	mapCtx := &direct.MapContext{}

	familySubset := make(map[string]*krm.AuthorizedView_FamilySubsets)
	for key, value := range a.actual.AuthorizedView.(*gcp.SubsetViewInfo).FamilySubsets {
		familySubset[key] = &krm.AuthorizedView_FamilySubsets{
			Qualifiers:        value.Qualifiers,
			QualifierPrefixes: value.QualifierPrefixes,
		}
	}
	subsetView := &krm.AuthorizedView_SubsetView{
		RowPrefixes:   a.actual.AuthorizedView.(*gcp.SubsetViewInfo).RowPrefixes,
		FamilySubsets: familySubset,
	}

	pbSubsetView := &krm.BigtableAuthorizedViewSpec{
		ResourceID: PtrTo(a.actual.AuthorizedViewID),
		SubsetView: subsetView,
	}

	obj.Spec = direct.ValueOf(pbSubsetView)
	if mapCtx.Err() != nil {
		return nil, mapCtx.Err()
	}

	uObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}

	u.SetName(a.id.ID())
	u.SetGroupVersionKind(krm.BigtableAuthorizedViewGVK)

	u.Object = uObj
	return u, nil
}

// Delete the resource from GCP service when the corresponding Config Connector resource is deleted.
func (a *BigtableAuthorizedViewAdapter) Delete(ctx context.Context, deleteOp *directbase.DeleteOperation) (bool, error) {
	log := klog.FromContext(ctx)
	log.V(2).Info("deleting BigtableAuthorizedView", "name", a.id)

	err := a.gcpClient.DeleteAuthorizedView(ctx, a.id.Parent().ID(), a.id.ID())
	if err != nil {
		if direct.IsNotFound(err) {
			// Return success if not found (assume it was already deleted).
			log.V(2).Info("skipping delete for non-existent BigtableAuthorizedView, assuming it was already deleted", "name", a.id)
			return true, nil
		}
		return false, fmt.Errorf("deleting BigtableAuthorizedView %s: %w", a.id, err)
	}
	log.V(2).Info("successfully deleted BigtableAuthorizedView", "name", a.id)

	return true, nil
}

func (a *BigtableAuthorizedViewAdapter) GetIAMPolicy(ctx context.Context) (*iampb.Policy, error) {
	if a.id == nil {
		return nil, fmt.Errorf("cannot get iam policy for missing resource")
	}

	iH := a.gcpClient.AuthorizedViewIAM(a.id.Parent().ID(), a.id.ID())
	policy, err := iH.Policy(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting iam policy for %q: %w", a.fullyQualifiedName(), err)
	}

	return policy.InternalProto, nil
}

func (a *BigtableAuthorizedViewAdapter) SetIAMPolicy(ctx context.Context, policy *iampb.Policy) (*iampb.Policy, error) {
	if a.id == nil {
		return nil, fmt.Errorf("cannot get iam policy for missing resource")
	}

	iH := a.gcpClient.AuthorizedViewIAM(a.id.Parent().ID(), a.id.ID())
	iamPolicy := &iam.Policy{InternalProto: policy}
	err := iH.SetPolicy(ctx, iamPolicy)
	if err != nil {
		return nil, fmt.Errorf("setting iam policy for %q: %w", a.fullyQualifiedName(), err)
	}

	return policy, nil
}

func (a *BigtableAuthorizedViewAdapter) fullyQualifiedName() string {
	return fmt.Sprintf("projects/%s/instances/%s/tables/%s/authorizedViews/%s", a.id.Parent().Parent.Parent.ProjectID, a.id.Parent().Parent.Id, a.id.Parent().ID(), a.id.ID())
}
