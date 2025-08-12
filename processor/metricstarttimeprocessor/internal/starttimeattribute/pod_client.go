package starttimeattribute

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/k8sconfig"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/informers"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type k8sPodClient struct {
	clientset      k8s.Interface
	informerStop   chan struct{}
	informer       cache.SharedIndexInformer
	mu             sync.RWMutex
	podByIP        map[string]*corev1.Pod
	podByName      map[string]*corev1.Pod
	podByUID       map[string]*corev1.Pod
	startTimeCache map[string]time.Time
}

func newK8sPodClient(ctx context.Context, apiConfig k8sconfig.APIConfig, filter informerFilter) (podClient, error) {
	clientset, err := k8sconfig.MakeClient(apiConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	labelSelector := labels.Everything()
	if len(filter.LabelFilters) > 0 {
		for _, lf := range filter.LabelFilters {
			requirement, err := labels.NewRequirement(lf.Key, lf.Op, []string{lf.Value})
			if err != nil {
				return nil, fmt.Errorf("failed to create label requirement: %w", err)
			}
			labelSelector = labelSelector.Add(*requirement)
		}
	}

	fieldSelector := fields.Everything()
	if len(filter.FieldFilters) > 0 {
		fieldSet := fields.Set{}
		for _, ff := range filter.FieldFilters {
			if ff.Op == selection.Equals {
				fieldSet[ff.Key] = ff.Value
			}
		}
		if len(fieldSet) > 0 {
			fieldSelector = fields.SelectorFromSet(fieldSet)
		}
	}

	options := informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
		opts.LabelSelector = labelSelector.String()
		opts.FieldSelector = fieldSelector.String()
	})

	var factory informers.SharedInformerFactory
	if filter.Namespace != "" {
		factory = informers.NewSharedInformerFactoryWithOptions(
			clientset,
			time.Hour,
			informers.WithNamespace(filter.Namespace),
			options,
		)
	} else {
		factory = informers.NewSharedInformerFactoryWithOptions(
			clientset,
			time.Hour,
			options,
		)
	}

	podInformer := factory.Core().V1().Pods().Informer()

	client := &k8sPodClient{
		clientset:      clientset,
		informerStop:   make(chan struct{}),
		informer:       podInformer,
		podByIP:        make(map[string]*corev1.Pod),
		podByName:      make(map[string]*corev1.Pod),
		podByUID:       make(map[string]*corev1.Pod),
		startTimeCache: make(map[string]time.Time),
	}

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				client.addPod(pod)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if oldPod, ok := oldObj.(*corev1.Pod); ok {
				client.deletePod(oldPod)
			}
			if newPod, ok := newObj.(*corev1.Pod); ok {
				client.addPod(newPod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				client.deletePod(pod)
			}
		},
	})

	factory.Start(client.informerStop)
	factory.WaitForCacheSync(client.informerStop)

	return client, nil
}

func (c *k8sPodClient) addPod(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if pod.Status.PodIP != "" {
		c.podByIP[pod.Status.PodIP] = pod
	}

	podName := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	c.podByName[podName] = pod

	c.podByUID[string(pod.UID)] = pod

	if pod.Status.StartTime != nil {
		startTime := pod.Status.StartTime.Time
		if pod.Status.PodIP != "" {
			c.startTimeCache[fmt.Sprintf("ip:%s", pod.Status.PodIP)] = startTime
		}
		c.startTimeCache[fmt.Sprintf("name:%s", podName)] = startTime
		c.startTimeCache[fmt.Sprintf("uid:%s", pod.UID)] = startTime
	}
}

func (c *k8sPodClient) deletePod(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if pod.Status.PodIP != "" {
		delete(c.podByIP, pod.Status.PodIP)
		delete(c.startTimeCache, fmt.Sprintf("ip:%s", pod.Status.PodIP))
	}

	podName := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	delete(c.podByName, podName)
	delete(c.startTimeCache, fmt.Sprintf("name:%s", podName))

	delete(c.podByUID, string(pod.UID))
	delete(c.startTimeCache, fmt.Sprintf("uid:%s", pod.UID))
}

func (c *k8sPodClient) GetPodStartTime(ctx context.Context, podID podIdentifier) (time.Time, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var pod *corev1.Pod
	var cacheKey string

	switch podID.Type {
	case podIP:
		pod = c.podByIP[podID.Value]
		cacheKey = fmt.Sprintf("ip:%s", podID.Value)
	case podName:
		pod = c.podByName[podID.Value]
		cacheKey = fmt.Sprintf("name:%s", podID.Value)
	case podUID:
		pod = c.podByUID[podID.Value]
		cacheKey = fmt.Sprintf("uid:%s", podID.Value)
	default:
		return time.Time{}, fmt.Errorf("unknown pod identifier type: %v", podID.Type)
	}

	if startTime, ok := c.startTimeCache[cacheKey]; ok {
		return startTime, nil
	}

	if pod == nil {
		return time.Time{}, fmt.Errorf("pod not found for identifier: %s", podID.Value)
	}

	if pod.Status.StartTime == nil {
		return time.Time{}, fmt.Errorf("pod %s has no start time", podID.Value)
	}

	return pod.Status.StartTime.Time, nil
}

func (c *k8sPodClient) Stop() {
	close(c.informerStop)
}