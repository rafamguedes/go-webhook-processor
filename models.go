package main

type Event struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	RequestID string         `json:"requestId,omitempty"`
	Payload   map[string]any `json:"payload"`
}

type HealthResponse struct {
	Status        string `json:"status"`
	Time          string `json:"time"`
	Date          string `json:"date"`
	QueueLength   int    `json:"queueLength"`
	QueueCapacity int    `json:"queueCapacity"`
}
