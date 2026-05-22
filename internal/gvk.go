// Package internal contains shared utilities used across KubeClient packages.
// This file provides GroupVersionKind helper functions.
package internal

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GVKToString returns a human-readable string for a GVK, e.g. "apps/v1/Deployment".
func GVKToString(gvk schema.GroupVersionKind) string {
	if gvk.Group == "" {
		return fmt.Sprintf("%s/%s", gvk.Version, gvk.Kind)
	}
	return fmt.Sprintf("%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind)
}

// GVRFromGVK converts a GVK to a GVR (GroupVersionResource) using a simple pluralization rule.
// This is a best-effort conversion: it lowercases the Kind and appends "s".
// For production use, a RESTMapper should be used instead.
func GVRFromGVK(gvk schema.GroupVersionKind) schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    gvk.Group,
		Version:  gvk.Version,
		Resource: strings.ToLower(gvk.Kind) + "s",
	}
}
