package kube

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// Client bundles the Kubernetes clientset and REST config used for streaming.
type Client struct {
	Clientset  kubernetes.Interface
	Metadata   metadata.Interface
	RestConfig *rest.Config
}

// New constructs an in-cluster Kubernetes clientset.
func New() (*Client, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create clientset: %w", err)
	}

	metadataClient, err := metadata.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create metadata client: %w", err)
	}

	return &Client{Clientset: clientset, Metadata: metadataClient, RestConfig: config}, nil
}
