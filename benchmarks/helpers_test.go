// Package benchmarks contains benchmarks comparing kubeclient's dedup store
// against a plain map (simulating go-client / controller-runtime in-memory cache).
package benchmarks

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ldmonster/kubeclient/store"
)

// podGVK is the GVK used for all benchmark Pod objects.
var podGVK = schema.GroupVersionKind{
	Group:   "",
	Version: "v1",
	Kind:    "Pod",
}

// makePod creates a realistic Pod *unstructured.Unstructured.
// Only the name and namespace differ between pods; all other fields are
// identical so the dedup store can maximally share subtrees.
func makePod(name, namespace string, i int) *unstructured.Unstructured {
	u := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
				"labels": map[string]interface{}{
					"app":     "myapp",
					"version": "v1",
					"env":     "production",
				},
				"annotations": map[string]interface{}{
					"kubectl.kubernetes.io/last-applied-configuration": "{}",
					"prometheus.io/scrape":                             "true",
					"prometheus.io/port":                               "8080",
				},
				"resourceVersion": fmt.Sprintf("%d", 1000+i),
				"uid":             fmt.Sprintf("aaaaaaaa-bbbb-cccc-dddd-%012d", i),
				"generation":      int64(1),
			},
			"spec": map[string]interface{}{
				"restartPolicy":                 "Always",
				"terminationGracePeriodSeconds": int64(30),
				"dnsPolicy":                     "ClusterFirst",
				"serviceAccountName":            "default",
				"nodeName":                      "node-1",
				"schedulerName":                 "default-scheduler",
				"tolerations": []interface{}{
					map[string]interface{}{
						"key":               "node.kubernetes.io/not-ready",
						"operator":          "Exists",
						"effect":            "NoExecute",
						"tolerationSeconds": int64(300),
					},
					map[string]interface{}{
						"key":               "node.kubernetes.io/unreachable",
						"operator":          "Exists",
						"effect":            "NoExecute",
						"tolerationSeconds": int64(300),
					},
				},
				"containers": []interface{}{
					map[string]interface{}{
						"name":  "nginx",
						"image": "nginx:1.21",
						"ports": []interface{}{
							map[string]interface{}{
								"containerPort": int64(80),
								"protocol":      "TCP",
							},
							map[string]interface{}{
								"containerPort": int64(443),
								"protocol":      "TCP",
							},
						},
						"resources": map[string]interface{}{
							"requests": map[string]interface{}{
								"cpu":    "100m",
								"memory": "128Mi",
							},
							"limits": map[string]interface{}{
								"cpu":    "500m",
								"memory": "512Mi",
							},
						},
						"env": []interface{}{
							map[string]interface{}{
								"name":  "POD_NAME",
								"value": name,
							},
							map[string]interface{}{
								"name":  "POD_NAMESPACE",
								"value": namespace,
							},
							map[string]interface{}{
								"name":  "APP_ENV",
								"value": "production",
							},
						},
						"volumeMounts": []interface{}{
							map[string]interface{}{
								"name":      "config",
								"mountPath": "/etc/nginx/conf.d",
								"readOnly":  true,
							},
						},
						"livenessProbe": map[string]interface{}{
							"httpGet": map[string]interface{}{
								"path":   "/healthz",
								"port":   int64(8080),
								"scheme": "HTTP",
							},
							"initialDelaySeconds": int64(10),
							"periodSeconds":       int64(10),
							"successThreshold":    int64(1),
							"failureThreshold":    int64(3),
						},
						"readinessProbe": map[string]interface{}{
							"httpGet": map[string]interface{}{
								"path":   "/ready",
								"port":   int64(8080),
								"scheme": "HTTP",
							},
							"initialDelaySeconds": int64(5),
							"periodSeconds":       int64(5),
							"successThreshold":    int64(1),
							"failureThreshold":    int64(3),
						},
						"terminationMessagePath":   "/dev/termination-log",
						"terminationMessagePolicy": "File",
						"imagePullPolicy":          "IfNotPresent",
					},
				},
				"volumes": []interface{}{
					map[string]interface{}{
						"name": "config",
						"configMap": map[string]interface{}{
							"name":        "nginx-config",
							"defaultMode": int64(420),
						},
					},
				},
				"securityContext": map[string]interface{}{
					"runAsNonRoot": true,
					"runAsUser":    int64(1000),
					"fsGroup":      int64(2000),
				},
			},
			"status": map[string]interface{}{
				"phase":  "Running",
				"hostIP": "192.168.1.1",
				"podIP":  "10.0.0.1",
				"conditions": []interface{}{
					map[string]interface{}{
						"type":               "Initialized",
						"status":             "True",
						"lastProbeTime":      nil,
						"lastTransitionTime": "2024-01-01T00:00:00Z",
					},
					map[string]interface{}{
						"type":               "Ready",
						"status":             "True",
						"lastProbeTime":      nil,
						"lastTransitionTime": "2024-01-01T00:00:10Z",
					},
					map[string]interface{}{
						"type":               "ContainersReady",
						"status":             "True",
						"lastProbeTime":      nil,
						"lastTransitionTime": "2024-01-01T00:00:10Z",
					},
					map[string]interface{}{
						"type":               "PodScheduled",
						"status":             "True",
						"lastProbeTime":      nil,
						"lastTransitionTime": "2024-01-01T00:00:00Z",
					},
				},
				"containerStatuses": []interface{}{
					map[string]interface{}{
						"name":         "nginx",
						"image":        "nginx:1.21",
						"imageID":      "docker-pullable://nginx@sha256:abc123",
						"containerID":  fmt.Sprintf("docker://container%012d", i),
						"ready":        true,
						"restartCount": int64(0),
						"state": map[string]interface{}{
							"running": map[string]interface{}{
								"startedAt": "2024-01-01T00:00:10Z",
							},
						},
					},
				},
				"qosClass": "Burstable",
			},
		},
	}
	return u
}

// makeObjectKey creates a store.ObjectKey from an unstructured object.
func makeObjectKey(u *unstructured.Unstructured) store.ObjectKey {
	gvk := u.GroupVersionKind()
	return store.ObjectKey{
		GVK:       gvk,
		Namespace: u.GetNamespace(),
		Name:      u.GetName(),
	}
}

// makePodTyped creates a realistic typed *corev1.Pod.
// Only name and namespace differ between pods; all other fields are identical
// so the controller-runtime Indexer stores full independent copies of each.
func makePodTyped(name, namespace string) *corev1.Pod {
	trueVal := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app":     "myapp",
				"version": "v1",
				"env":     "production",
			},
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": "{}",
				"prometheus.io/scrape":                             "true",
				"prometheus.io/port":                               "8080",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyAlways,
			TerminationGracePeriodSeconds: func() *int64 { v := int64(30); return &v }(),
			DNSPolicy:                     corev1.DNSClusterFirst,
			ServiceAccountName:            "default",
			NodeName:                      "node-1",
			SchedulerName:                 "default-scheduler",
			Tolerations: []corev1.Toleration{
				{
					Key:               "node.kubernetes.io/not-ready",
					Operator:          corev1.TolerationOpExists,
					Effect:            corev1.TaintEffectNoExecute,
					TolerationSeconds: func() *int64 { v := int64(300); return &v }(),
				},
				{
					Key:               "node.kubernetes.io/unreachable",
					Operator:          corev1.TolerationOpExists,
					Effect:            corev1.TaintEffectNoExecute,
					TolerationSeconds: func() *int64 { v := int64(300); return &v }(),
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx:1.21",
					Ports: []corev1.ContainerPort{
						{ContainerPort: 80, Protocol: corev1.ProtocolTCP},
						{ContainerPort: 443, Protocol: corev1.ProtocolTCP},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
					Env: []corev1.EnvVar{
						{Name: "POD_NAME", Value: name},
						{Name: "POD_NAMESPACE", Value: namespace},
						{Name: "APP_ENV", Value: "production"},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "config", MountPath: "/etc/nginx/conf.d", ReadOnly: true},
					},
					LivenessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path:   "/healthz",
								Port:   intOrString(8080),
								Scheme: corev1.URISchemeHTTP,
							},
						},
						InitialDelaySeconds: 10,
						PeriodSeconds:       10,
						SuccessThreshold:    1,
						FailureThreshold:    3,
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path:   "/ready",
								Port:   intOrString(8080),
								Scheme: corev1.URISchemeHTTP,
							},
						},
						InitialDelaySeconds: 5,
						PeriodSeconds:       5,
						SuccessThreshold:    1,
						FailureThreshold:    3,
					},
					TerminationMessagePath:   "/dev/termination-log",
					TerminationMessagePolicy: corev1.TerminationMessageReadFile,
					ImagePullPolicy:          corev1.PullIfNotPresent,
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "nginx-config"},
							DefaultMode:          func() *int32 { v := int32(420); return &v }(),
						},
					},
				},
			},
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: &trueVal,
				RunAsUser:    func() *int64 { v := int64(1000); return &v }(),
				FSGroup:      func() *int64 { v := int64(2000); return &v }(),
			},
		},
		Status: corev1.PodStatus{
			Phase:  corev1.PodRunning,
			HostIP: "192.168.1.1",
			PodIP:  "10.0.0.1",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
}

// intOrString returns an intstr.IntOrString from an int value (used for probe ports).
func intOrString(v int) intstr.IntOrString {
	return intstr.FromInt32(int32(v))
}
