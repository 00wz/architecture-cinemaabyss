package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
)

// Event represents a generic event in the system
type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

// MovieEvent represents a movie-related event
type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"` // viewed, rated, added, etc.
	UserID      int      `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

// UserEvent represents a user-related event
type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"` // registered, logged_in, updated_profile, etc.
	Timestamp time.Time `json:"timestamp"`
}

// PaymentEvent represents a payment-related event
type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"` // completed, failed, refunded, etc.
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type,omitempty"`
}

var (
	producer sarama.SyncProducer
	consumer sarama.Consumer
)

func main() {
	// Initialize Kafka producer
	initKafkaProducer()
	defer producer.Close()

	// Initialize Kafka consumer
	initKafkaConsumer()
	defer consumer.Close()

	// Start consuming messages in the background
	go consumeMessages("movie-events")
	go consumeMessages("user-events")
	go consumeMessages("payment-events")

	// Set up HTTP routes
	router := mux.NewRouter()
	router.HandleFunc("/api/events/health", handleHealth).Methods("GET")
	router.HandleFunc("/api/events/movie", handleMovieEvent).Methods("POST")
	router.HandleFunc("/api/events/user", handleUserEvent).Methods("POST")
	router.HandleFunc("/api/events/payment", handlePaymentEvent).Methods("POST")

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting events service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, router))
}

func initKafkaProducer() {
	// Get Kafka broker address from environment variable or use default
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	if brokers[0] == "" {
		brokers[0] = "kafka:9092"
	}

	// Create Kafka producer configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// Create Kafka producer
	var err error
	producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	log.Println("Kafka producer initialized successfully")
}

func initKafkaConsumer() {
	// Get Kafka broker address from environment variable or use default
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	if brokers[0] == "" {
		brokers[0] = "kafka:9092"
	}

	// Create Kafka consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create Kafka consumer
	var err error
	consumer, err = sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	log.Println("Kafka consumer initialized successfully")
}

func consumeMessages(topic string) {
	// Create a consumer for the specified topic
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Failed to create partition consumer for topic %s: %v", topic, err)
		return
	}
	defer partitionConsumer.Close()

	log.Printf("Started consuming messages from topic: %s", topic)

	// Continuously consume messages
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Received message from topic %s: %s", topic, string(msg.Value))
			processMessage(topic, msg.Value)
		case err := <-partitionConsumer.Errors():
			log.Printf("Error consuming from topic %s: %v", topic, err)
		}
	}
}

func processMessage(topic string, message []byte) {
	// Process the message based on the topic
	switch topic {
	case "movie-events":
		var event Event
		if err := json.Unmarshal(message, &event); err != nil {
			log.Printf("Error unmarshaling movie event: %v", err)
			return
		}
		log.Printf("Processing movie event: %+v", event)
		// Implement business logic for movie events
	case "user-events":
		var event Event
		if err := json.Unmarshal(message, &event); err != nil {
			log.Printf("Error unmarshaling user event: %v", err)
			return
		}
		log.Printf("Processing user event: %+v", event)
		// Implement business logic for user events
	case "payment-events":
		var event Event
		if err := json.Unmarshal(message, &event); err != nil {
			log.Printf("Error unmarshaling payment event: %v", err)
			return
		}
		log.Printf("Processing payment event: %+v", event)
		// Implement business logic for payment events
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	var movieEvent MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&movieEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create a generic event with the movie event as payload
	event := Event{
		ID:        fmt.Sprintf("movie-%d-%s", movieEvent.MovieID, movieEvent.Action),
		Type:      "movie",
		Timestamp: time.Now(),
		Payload:   movieEvent,
	}

	// Convert event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send event to Kafka
	msg := &sarama.ProducerMessage{
		Topic: "movie-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Movie event sent to partition %d at offset %d", partition, offset)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	var userEvent UserEvent
	if err := json.NewDecoder(r.Body).Decode(&userEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create a generic event with the user event as payload
	event := Event{
		ID:        fmt.Sprintf("user-%d-%s", userEvent.UserID, userEvent.Action),
		Type:      "user",
		Timestamp: time.Now(),
		Payload:   userEvent,
	}

	// Convert event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send event to Kafka
	msg := &sarama.ProducerMessage{
		Topic: "user-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("User event sent to partition %d at offset %d", partition, offset)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	var paymentEvent PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&paymentEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create a generic event with the payment event as payload
	event := Event{
		ID:        fmt.Sprintf("payment-%d-%s", paymentEvent.PaymentID, paymentEvent.Status),
		Type:      "payment",
		Timestamp: time.Now(),
		Payload:   paymentEvent,
	}

	// Convert event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send event to Kafka
	msg := &sarama.ProducerMessage{
		Topic: "payment-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Payment event sent to partition %d at offset %d", partition, offset)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}
