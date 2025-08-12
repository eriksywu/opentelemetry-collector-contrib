# Description
The starttimeattribute package will provide an Adjuster implementation that will be called/instantiated in ../factory.go in the createdMetricsProcessor function.

The starttimeattribute Adjuster will:
- for any incoming pmetric.Metrics, it will at best effort find the associated k8s pod object's start time that the metric is associated with
- if pod object is found, it will extract the pod's start time and set it as the start time for all cumulative metric points in the pmetric.Metrics


In processor/metricstarttimeprocessor/internal/starttimeattribute/adjuster.go I have laid out the skeleton code for the Adjuster that implements the method prototypes needed to be called in factory.go.


# Implementation Steps
## 1. Implement the podClient interface in adjuster.go:
The concrete implementation of the podClient interface will be responsible for fetching the pod object from the Kubernetes API server. 
This will involve using a Kubernetes client to query for pods based on labels or other identifiers that match the incoming metrics
Implementation requirements:
- expose a constructor that follows the function prototype `podClientFactory` in adjuster.go
- use a sharedInformer under the hood to efficiently watch for pod events (adds, deletes and updates) and cache the pod objects 
  -- @../../../go/pkg/mod/k8s.io/client-go@v0.32.3/tools/cache/shared_informer.go contains factory methods to create a shared informer
- the underlying sharedInformer created should respect the `informerFilter` object that is passed via the `podClientFactory` prototype
- the implemented method `GetPodStartTime` should return the start time of the pod if it exists, or an error if it does not exist or cannot be fetched
- the implementation needs to be thread-safe
- note that the GetPodStartTime method uses a podIdentifier comparable type that has three different possible comparable fields: podIP, podName and podUID. 
This means the implementation should be able to handle any of these fields being set to identify the pod and thus cache pod objects or times based on these identifiers. 
- Be mindful of the above point and how it can have an impact of memory usage and performance.

## 2. Fill in the implementation for the Adjuster 
Add in code for the AdjustMetrics method
Logic:
- for every metrics in the pmetric.Metrics, check if it has a cumulative aggregation temporality
- if not, then this is a no-op and return the pmetric.Metrics as-is
- if it does, then for every metric point in the cumulative metric, call the podClient.GetPodStartTime method to get the pod start time and set it as the start time for the metric point
- if podClient.GetPodStartTime returns an error, log the error and return the pmetric.Metrics as-is
In the NewAdjuster function, instantiate the podClient using the podClientFactory and return the Adjuster with the podClient set.